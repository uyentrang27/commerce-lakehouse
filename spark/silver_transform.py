"""Silver layer (PySpark): conform multiple sources into one clean lake.

The business (a simulated refurbished-phone reseller) sells through several
marketplaces and settles cash through an order-management system. Each source
speaks a different language, so Silver's job is to make them comparable:

  SALES channels (Amazon, BackMarket) -- same grain (one sold order) but
    different column names, status codes, currency and date formats.
    -> conform each to one schema, then UNION into `silver/sales`.

  SETTLEMENT (OMS) -- a DIFFERENT grain (cash remitted per order, net of fees).
    -> conform but keep SEPARATE as `silver/settlements`; Gold reconciles it
       against sales by order reference.

Performance decisions worth defending in an interview:
- **Broadcast joins** for every reference table (sku map, status map, FX). They
  are tiny next to the million-row sales facts, so we ship them to each executor
  and the fact never shuffles.
- **Cast on read into a common schema** so both channels line up for a clean
  `unionByName`.
- **Cache** the unioned sales frame -- it is both counted and written, so we
  materialise it once instead of recomputing the whole conform+union lineage.
- **coalesce** before write to avoid the small-files problem; **partitionBy**
  order_date so downstream reads prune partitions.
"""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# common sales schema both channels conform to (order matters for unionByName)
SALES_COLS = ["order_id", "source", "product_id", "quantity", "unit_price",
              "canonical_status", "order_date", "revenue_usd"]


def get_spark(app: str = "commerce-silver") -> SparkSession:
    return (
        SparkSession.builder.appName(app)
        .config("spark.sql.shuffle.partitions", "16")   # small data -> 16, not 200
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read(spark: SparkSession, data_dir: str, name: str) -> DataFrame:
    # Silver reads the Bronze zone (not raw) -> Bronze sits inside the flow.
    return spark.read.parquet(f"{data_dir}/bronze/{name}.parquet")


def conform_fx(fx: DataFrame) -> DataFrame:
    """Small daily FX table -> broadcast-ready, renamed so keys don't collide."""
    return fx.select(
        F.col("currency").alias("fx_currency"),
        F.to_date("rate_date").alias("fx_date"),
        "rate_to_usd",
    )


def _to_usd(df: DataFrame, amount_col: str, out_col: str, cur_col: str,
            date_col: str, fx_conf: DataFrame) -> DataFrame:
    """Join the (currency, date) FX rate and convert `amount_col` to USD."""
    return (
        df.join(broadcast(fx_conf),
                (F.col(cur_col) == F.col("fx_currency"))
                & (F.col(date_col) == F.col("fx_date")), how="left")
        .withColumn(out_col, F.round(F.col(amount_col) * F.col("rate_to_usd"), 2))
        .drop("fx_currency", "fx_date", "rate_to_usd")
    )


def conform_amazon(sales, sku_map, status_map, fx_conf) -> DataFrame:
    """Amazon: USD, string status, date 'YYYY/MM/DD', product key = asin."""
    df = (
        sales
        .withColumnRenamed("amazon_order_id", "order_id")
        .withColumnRenamed("asin", "source_sku")
        .withColumnRenamed("qty", "quantity")
        .withColumnRenamed("item_price", "unit_price")
        .withColumnRenamed("order_status", "raw_status")
        .withColumn("source", F.lit("amazon"))
        .withColumn("order_date", F.to_date("purchase_date", "yyyy/MM/dd"))
        .join(broadcast(sku_map.filter(F.col("channel") == "amazon")
                        .select("source_sku", "product_id")), on="source_sku", how="left")
        .join(broadcast(status_map.filter(F.col("channel") == "amazon")
                        .select("raw_status", "canonical_status")), on="raw_status", how="left")
        .withColumn("gross", F.col("quantity") * F.col("unit_price"))
    )
    df = _to_usd(df, "gross", "revenue_usd", "currency", "order_date", fx_conf)
    return df.select(*SALES_COLS)


def conform_backmarket(sales, sku_map, status_map, fx_conf) -> DataFrame:
    """BackMarket: EUR, INT status code, date 'DD-MM-YYYY', key = product_sku."""
    df = (
        sales
        .withColumnRenamed("bm_order_ref", "order_id")
        .withColumnRenamed("product_sku", "source_sku")
        .withColumnRenamed("unit_amount", "unit_price")
        .withColumnRenamed("devise", "currency")
        .withColumn("source", F.lit("backmarket"))
        .withColumn("order_date", F.to_date("date_commande", "dd-MM-yyyy"))
        # state is an INT (3); status_map.raw_status is a STRING ('3') -> cast
        .withColumn("raw_status", F.col("state").cast("string"))
        .join(broadcast(sku_map.filter(F.col("channel") == "backmarket")
                        .select("source_sku", "product_id")), on="source_sku", how="left")
        .join(broadcast(status_map.filter(F.col("channel") == "backmarket")
                        .select("raw_status", "canonical_status")), on="raw_status", how="left")
        .withColumn("gross", F.col("quantity") * F.col("unit_price"))
    )
    df = _to_usd(df, "gross", "revenue_usd", "currency", "order_date", fx_conf)
    return df.select(*SALES_COLS)


def conform_settlement(settle, fx_conf) -> DataFrame:
    """OMS settlement (different grain). Parse epoch seconds, convert to USD."""
    df = (
        settle
        .withColumn("payout_date", F.to_date(F.from_unixtime("payout_ts")))
        .withColumn("fees_raw", F.col("marketplace_fee") + F.col("shipping_fee"))
    )
    df = _to_usd(df, "gross_amount", "gross_usd", "payout_currency", "payout_date", fx_conf)
    df = _to_usd(df, "fees_raw", "fees_usd", "payout_currency", "payout_date", fx_conf)
    df = _to_usd(df, "net_paid", "net_usd", "payout_currency", "payout_date", fx_conf)
    return df.select("settlement_id", "channel", "order_ref", "payout_date",
                     "gross_usd", "fees_usd", "net_usd")


def conform_products(products) -> DataFrame:
    """Pass the product master through Silver so Gold reads one clean layer."""
    return products.dropDuplicates(["product_id"]).select(
        "product_id", "model_name", "brand", "grade",
        F.col("unit_cost").cast("double"), F.col("list_price").cast("double"))


def write_silver(df: DataFrame, out: str, name: str, partition_by=None, files=4):
    writer = df.coalesce(files).write.mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(f"{out}/silver/{name}")
    print(f"[silver] wrote {name}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    args = ap.parse_args()
    data_dir = out = args.data_dir

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    sku_map = read(spark, data_dir, "ref_sku_map")
    status_map = read(spark, data_dir, "ref_status_map")
    fx_conf = conform_fx(read(spark, data_dir, "ref_fx"))

    amz = conform_amazon(read(spark, data_dir, "sales_amazon"), sku_map, status_map, fx_conf)
    bm = conform_backmarket(read(spark, data_dir, "sales_backmarket"), sku_map, status_map, fx_conf)
    silver_sales = amz.unionByName(bm).cache()          # counted + written -> cache

    silver_settle = conform_settlement(read(spark, data_dir, "settlements_oms"), fx_conf)
    silver_products = conform_products(read(spark, data_dir, "ref_product"))

    print(f"[silver] sales={silver_sales.count():,}")    # single count off the cache

    write_silver(silver_sales, out, "sales", partition_by=["order_date"])
    write_silver(silver_settle, out, "settlements")
    write_silver(silver_products, out, "products", files=1)

    spark.stop()
    print("[silver] done")


if __name__ == "__main__":
    main()
