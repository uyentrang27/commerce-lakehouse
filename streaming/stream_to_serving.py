"""Real-time serving: Kafka order events -> per-minute aggregate -> Postgres.

This is the streaming counterpart of the batch serving load. Where
`stream_to_bronze.py` lands raw events for later conforming, this job keeps a
live **orders-per-minute by channel** aggregate that Grafana auto-refreshes —
the real-time ops view next to the batch reconciliation dashboard.

Design notes worth defending:
  * **Event-time window + watermark.** Aggregation is on the event's own
    timestamp (`window(event_ts, '1 minute')`) with a watermark, so late events
    still land in the right minute and state is bounded.
  * **`foreachBatch` -> Postgres upsert.** Each micro-batch's updated windows are
    upserted (INSERT ... ON CONFLICT) keyed by (window_start, channel), so a
    re-run or a late event corrects the bucket instead of duplicating it.
  * **Postgres, not DuckDB, is the real-time sink** — DuckDB is single-writer, so
    concurrent stream-write + dashboard-read would block; Postgres handles both.
  * **Volume metrics only** (orders, units) — the stream is raw Bronze, *before*
    Silver's FX conform, so summing mixed-currency revenue here would be wrong.

    python streaming/stream_to_serving.py --mode batch       # drain + stop
    python streaming/stream_to_serving.py --mode continuous  # live, every 5s
"""
from __future__ import annotations

import argparse
import os

import psycopg2
from psycopg2.extras import execute_batch
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

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

DDL = """
CREATE SCHEMA IF NOT EXISTS serving;
CREATE TABLE IF NOT EXISTS serving.orders_stream_1m (
    window_start timestamptz NOT NULL,
    window_end   timestamptz NOT NULL,
    channel      text        NOT NULL,
    orders       bigint,
    units        bigint,
    updated_at   timestamptz,
    PRIMARY KEY (window_start, channel)
);
"""

UPSERT = """
INSERT INTO serving.orders_stream_1m
    (window_start, window_end, channel, orders, units, updated_at)
VALUES (%s, %s, %s, %s, %s, now())
ON CONFLICT (window_start, channel) DO UPDATE
    SET orders = EXCLUDED.orders,
        units  = EXCLUDED.units,
        updated_at = now();
"""


def ensure_table(dsn: str) -> None:
    conn = psycopg2.connect(dsn)
    with conn, conn.cursor() as cur:
        cur.execute(DDL)
    conn.close()


def make_writer(dsn: str):
    def write_batch(batch_df, _epoch_id: int) -> None:
        rows = [
            (r["window_start"], r["window_end"], r["channel"], int(r["orders"]), int(r["units"]))
            for r in batch_df.collect()
        ]
        if not rows:
            return
        conn = psycopg2.connect(dsn)
        with conn, conn.cursor() as cur:
            execute_batch(cur, UPSERT, rows)
        conn.close()
        print(f"[stream-serving] upserted {len(rows)} window rows")
    return write_batch


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--mode", choices=["batch", "continuous"], default="batch")
    ap.add_argument("--interval", default="5 seconds")
    ap.add_argument(
        "--pg",
        default=os.environ.get(
            "PG_DSN", "host=localhost port=5432 dbname=serving user=grafana password=grafana"
        ),
    )
    args = ap.parse_args()

    ensure_table(args.pg)

    spark = (
        SparkSession.builder
        .appName("commerce-lakehouse-stream-serving")
        .config("spark.jars.packages", KAFKA_PKG)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    events = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_ts", F.to_timestamp("event_ts"))
    )

    agg = (
        events
        .withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "1 minute").alias("w"), "channel")
        .agg(F.count("*").alias("orders"), F.sum("qty").alias("units"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "channel", "orders", "units",
        )
    )

    writer = (
        agg.writeStream
        .outputMode("update")
        .foreachBatch(make_writer(args.pg))
        .option("checkpointLocation", os.path.abspath("data/_ckpt_stream_serving"))
    )
    query = writer.trigger(availableNow=True).start() if args.mode == "batch" \
        else writer.trigger(processingTime=args.interval).start()

    print(f"[stream-serving] {args.mode}: {args.topic} -> Postgres serving.orders_stream_1m")
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
