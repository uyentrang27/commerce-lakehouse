"""Spark Structured Streaming — Kafka order events -> Bronze (streaming lane).

Reads the live `orders` topic, parses each JSON event, stamps ingestion audit
columns, and appends to the Bronze zone as partitioned Parquet with a
**checkpoint**. The checkpoint tracks the Kafka offsets already committed to the
file sink, so:
  * a crash/restart resumes exactly where it left off (fault tolerance), and
  * re-running after new events processes only the new offsets — no duplicates,
    no reprocessing (exactly-once to the file sink).

Structured Streaming is **micro-batch**: each trigger reads a bounded chunk of
Kafka and writes one Parquet commit. `--mode batch` uses `availableNow` (drain
everything currently on the topic, then stop) — deterministic, CI-friendly.
`--mode continuous` keeps running on a fixed interval like a real deployment.

    python streaming/stream_to_bronze.py --mode batch     # drain + stop
    python streaming/stream_to_bronze.py --mode continuous # run live

The landed events share the Bronze zone with the batch tables; conforming them
is the same Silver code the batch lane already runs — one lakehouse, two speeds.
"""
from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Kafka connector must match the bundled Spark + Scala build (4.1.2 / Scala 2.13).
KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("order_id", StringType()),
    StructField("channel", StringType()),
    StructField("sku", StringType()),
    StructField("qty", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("currency", StringType()),
    StructField("status", StringType()),
    StructField("event_ts", StringType()),
])


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--mode", choices=["batch", "continuous"], default="batch")
    ap.add_argument("--interval", default="10 seconds", help="trigger interval in continuous mode")
    ap.add_argument("--out", default="data/bronze_stream")
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out)
    sink = os.path.join(out_dir, "orders")
    checkpoint = os.path.join(out_dir, "_checkpoint", "orders")

    spark = (
        SparkSession.builder
        .appName("commerce-lakehouse-stream-bronze")
        .config("spark.jars.packages", KAFKA_PKG)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Kafka gives us key/value bytes + metadata. Parse the JSON payload and keep
    # the Kafka coordinates (partition/offset) as lineage, plus an ingestion
    # timestamp and an event_date to partition Bronze by.
    events = (
        raw.select(
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"),
        )
        .select("kafka_partition", "kafka_offset", "kafka_ts", "e.*")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    writer = (
        events.writeStream
        .format("parquet")
        .option("path", sink)
        .option("checkpointLocation", checkpoint)
        .partitionBy("event_date")
        .outputMode("append")
    )
    if args.mode == "batch":
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime=args.interval).start()

    print(f"[stream] {args.mode}: {args.topic} -> {sink} (checkpoint {checkpoint})")
    query.awaitTermination()

    if args.mode == "batch":
        # summarise what the drain landed (batch read of the sink)
        total = spark.read.parquet(sink).count()
        print(f"[stream] Bronze stream now holds {total:,} events at {sink}")
    spark.stop()


if __name__ == "__main__":
    main()
