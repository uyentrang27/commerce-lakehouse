"""Silver layer (PySpark): clean + conform + enrich the raw Bronze Parquet into
partitioned Silver Parquet.

Performance decisions worth defending in an interview:
- **Broadcast joins** for the small dimensions (marketplaces ~6 rows, products
  ~thousands). A dimension is tiny next to the fact, so we ship it to every
  executor and skip a full shuffle of the fact table.
- **Count once, reuse.** Row counts are computed a single time from a cached
  frame instead of calling `.count()` repeatedly (each call is a full scan).
- **File sizing.** Output is `coalesce`d before write to avoid the small-files
  problem (thousands of tiny Parquet files kill read performance downstream).
- **Partitioning.** Facts are partitioned by `order_date` so downstream reads
  prune partitions instead of scanning everything.
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast


def get_spark(app: str = "commerce-silver") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read(spark: SparkSession, raw: str, name: str) -> DataFrame:
    return spark.read.parquet(f"{raw}/raw/{name}.parquet")


def conform_dims(marketplaces, customers, products):
    """Type-cast + dedup dimensions on their natural keys."""
    marketplaces = marketplaces.dropDuplicates(["marketplace_id"])
    customers = customers.dropDuplicates(["customer_id"])
    products = (
        products.dropDuplicates(["product_id"])
        .withColumn("margin", F.round(F.col("list_price") - F.col("unit_cost"), 2))
    )
    return marketplaces, customers, products


def build_silver_orders(orders, marketplaces) -> DataFrame:
    # Broadcast the tiny marketplace dim: no shuffle of the big orders fact.
    return (
        orders.dropDuplicates(["order_id"])
        .join(broadcast(marketplaces.select(
            "marketplace_id", "marketplace_name", "channel_type", "commission_rate")),
            on="marketplace_id", how="left")
        .withColumn("is_returned", F.col("order_status") == F.lit("returned"))
        .withColumn("is_cancelled", F.col("order_status") == F.lit("cancelled"))
    )


def build_silver_items(order_items, products) -> DataFrame:
    # products is a dimension -> broadcast it rather than shuffle the item fact.
    return (
        order_items.dropDuplicates(["order_item_id"])
        .join(broadcast(products.select("product_id", "category", "brand", "unit_cost")),
              on="product_id", how="left")
        .withColumn("gross_amount",
                    F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("net_amount",
                    F.round(F.col("gross_amount") * (1 - F.col("discount_pct")), 2))
        .withColumn("cost_amount",
                    F.round(F.col("quantity") * F.col("unit_cost"), 2))
        .withColumn("margin_amount",
                    F.round(F.col("net_amount") - F.col("cost_amount"), 2))
    )


def write_silver(df: DataFrame, out: str, name: str, partition_by=None, files: int = 4):
    writer = df.coalesce(files).write.mode("overwrite")  # control file count
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(f"{out}/silver/{name}")
    print(f"[silver] wrote {name}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    args = ap.parse_args()
    raw = out = args.data_dir

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    marketplaces = read(spark, raw, "marketplaces")
    customers = read(spark, raw, "customers")
    products = read(spark, raw, "products")
    orders = read(spark, raw, "orders")
    order_items = read(spark, raw, "order_items")

    marketplaces, customers, products = conform_dims(marketplaces, customers, products)

    silver_orders = build_silver_orders(orders, marketplaces).cache()
    silver_items = build_silver_items(order_items, products).cache()

    # Count once from the cached frames (not repeated full scans).
    n_orders = silver_orders.count()
    n_items = silver_items.count()
    print(f"[silver] orders={n_orders:,} items={n_items:,}")

    write_silver(silver_orders, out, "orders", partition_by=["order_date"])
    write_silver(silver_items, out, "order_items")
    # Pass conformed dims through to Silver so Gold reads one clean layer.
    write_silver(marketplaces, out, "marketplaces", files=1)
    write_silver(customers, out, "customers", files=2)
    write_silver(products, out, "products", files=1)

    spark.stop()
    print("[silver] done")


if __name__ == "__main__":
    main()
