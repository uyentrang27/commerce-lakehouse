"""Synthetic multi-marketplace commerce data generator.

Models a Qwikfone-style business: one retailer selling across several
marketplaces. Vectorised with numpy so it scales to millions of orders on a
laptop.

Outputs Parquet into <out>/raw/:
    marketplaces.parquet   small dimension (broadcast-join candidate in Spark)
    customers.parquet      dimension; `city` mutates over time -> SCD2 demo
    products.parquet       dimension; `list_price` mutates over time -> SCD2 demo
    orders.parquet         fact grain: one row per order, dated over a window
    order_items.parquet    fact grain: one row per (order, product)
    payments.parquet       fact grain: one row per order

Flags:
    --scale N         number of orders (default 1_000_000)
    --days D          spread orders over the last D days (default 90)
    --mutate-dims     regenerate customer/product dims with ~5% changed
                      city/list_price (run this before a 2nd dbt snapshot to
                      produce SCD Type-2 history)
    --seed S          reproducibility
"""
from __future__ import annotations

import argparse
import pathlib
from datetime import date, timedelta

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

MARKETPLACES = [
    # name, country, channel_type, commission_rate
    ("Amazon",       "US", "marketplace", 0.15),
    ("Shopee",       "VN", "marketplace", 0.10),
    ("Lazada",       "VN", "marketplace", 0.11),
    ("TikTok Shop",  "VN", "social",      0.08),
    ("Back Market",  "EU", "marketplace", 0.12),
    ("Own Webstore", "VN", "direct",      0.00),
]
CATEGORIES = ["smartphone", "laptop", "tablet", "accessory", "wearable", "audio"]
BRANDS = ["Apple", "Samsung", "Xiaomi", "Oppo", "Dell", "Asus", "Sony", "Generic"]
SEGMENTS = ["consumer", "business", "reseller"]
CITIES = ["Ho Chi Minh City", "Hanoi", "Da Nang", "Can Tho", "Hai Phong",
          "Bien Hoa", "Nha Trang", "Hue"]
STATUSES = ["delivered", "shipped", "processing", "cancelled", "returned"]
STATUS_P = [0.62, 0.12, 0.10, 0.08, 0.08]
PAY_METHODS = ["credit_card", "e_wallet", "cod", "bank_transfer", "installment"]
CURRENCIES = ["VND", "USD", "EUR"]


def write(table_dict: dict, path: pathlib.Path) -> int:
    tbl = pa.table(table_dict)
    pq.write_table(tbl, path)
    return tbl.num_rows


def gen_marketplaces(out: pathlib.Path) -> None:
    names, countries, channels, comm = zip(*MARKETPLACES)
    n = write(
        {
            "marketplace_id": np.arange(1, len(MARKETPLACES) + 1, dtype="int32"),
            "marketplace_name": list(names),
            "country": list(countries),
            "channel_type": list(channels),
            "commission_rate": np.array(comm, dtype="float64"),
        },
        out / "marketplaces.parquet",
    )
    print(f"[gen] marketplaces: {n}")


def gen_customers(rng, n_customers: int, out: pathlib.Path, mutate: bool) -> None:
    ids = np.arange(1, n_customers + 1, dtype="int64")
    city_idx = rng.integers(0, len(CITIES), n_customers)
    if mutate:
        # ~5% of customers relocate -> new city (SCD2 change event)
        moved = rng.random(n_customers) < 0.05
        city_idx = np.where(moved, rng.integers(0, len(CITIES), n_customers), city_idx)
    seg_idx = rng.integers(0, len(SEGMENTS), n_customers)
    signup = date(2021, 1, 1)
    signup_days = rng.integers(0, 1600, n_customers)
    write(
        {
            "customer_id": ids,
            "full_name": [f"Customer {i}" for i in ids],
            "email": [f"cust{i}@example.com" for i in ids],
            "city": np.array(CITIES)[city_idx],
            "country": np.where(np.array(CITIES)[city_idx] == "", "VN", "VN"),
            "segment": np.array(SEGMENTS)[seg_idx],
            "signup_date": [str(signup + timedelta(days=int(d))) for d in signup_days],
        },
        out / "customers.parquet",
    )
    print(f"[gen] customers: {n_customers}{' (mutated cities)' if mutate else ''}")


def gen_products(rng, n_products: int, out: pathlib.Path, mutate: bool) -> None:
    ids = np.arange(1, n_products + 1, dtype="int64")
    cat_idx = rng.integers(0, len(CATEGORIES), n_products)
    brand_idx = rng.integers(0, len(BRANDS), n_products)
    unit_cost = np.round(rng.uniform(20, 1500, n_products), 2)
    markup = rng.uniform(1.15, 1.8, n_products)
    list_price = np.round(unit_cost * markup, 2)
    if mutate:
        # ~8% of products get a price change (SCD2 change event)
        repriced = rng.random(n_products) < 0.08
        list_price = np.where(
            repriced, np.round(list_price * rng.uniform(0.85, 1.25, n_products), 2),
            list_price,
        )
    write(
        {
            "product_id": ids,
            "product_name": [f"SKU-{i:06d}" for i in ids],
            "category": np.array(CATEGORIES)[cat_idx],
            "brand": np.array(BRANDS)[brand_idx],
            "supplier": [f"Supplier {b}" for b in np.array(BRANDS)[brand_idx]],
            "unit_cost": unit_cost,
            "list_price": list_price,
        },
        out / "products.parquet",
    )
    print(f"[gen] products: {n_products}{' (mutated prices)' if mutate else ''}")


def gen_facts(rng, n_orders: int, n_customers: int, n_products: int,
              days: int, out: pathlib.Path) -> None:
    order_ids = np.arange(1, n_orders + 1, dtype="int64")
    # order dates spread over the last `days` days
    end = date.today()
    day_offsets = rng.integers(0, days, n_orders)
    order_dates = np.array(
        [str(end - timedelta(days=int(d))) for d in range(days)]
    )[day_offsets]
    status_idx = rng.choice(len(STATUSES), n_orders, p=STATUS_P)

    write(
        {
            "order_id": order_ids,
            "customer_id": rng.integers(1, n_customers + 1, n_orders),
            "marketplace_id": rng.integers(1, len(MARKETPLACES) + 1, n_orders).astype("int32"),
            "order_date": order_dates,
            "order_status": np.array(STATUSES)[status_idx],
            "currency": np.array(CURRENCIES)[rng.choice(3, n_orders, p=[0.8, 0.15, 0.05])],
        },
        out / "orders.parquet",
    )
    print(f"[gen] orders: {n_orders}")

    # order_items: 1-4 items per order (vectorised expansion)
    items_per = rng.integers(1, 5, n_orders)
    total_items = int(items_per.sum())
    item_order_id = np.repeat(order_ids, items_per)
    write(
        {
            "order_item_id": np.arange(1, total_items + 1, dtype="int64"),
            "order_id": item_order_id,
            "product_id": rng.integers(1, n_products + 1, total_items),
            "quantity": rng.integers(1, 6, total_items).astype("int32"),
            "unit_price": np.round(rng.uniform(20, 2500, total_items), 2),
            "discount_pct": np.round(rng.choice([0, 0, 0, 5, 10, 15, 20], total_items) / 100.0, 2),
        },
        out / "order_items.parquet",
    )
    print(f"[gen] order_items: {total_items}")

    # payments: one per order
    write(
        {
            "payment_id": np.arange(1, n_orders + 1, dtype="int64"),
            "order_id": order_ids,
            "payment_method": np.array(PAY_METHODS)[rng.integers(0, len(PAY_METHODS), n_orders)],
            "amount": np.round(rng.uniform(20, 8000, n_orders), 2),
            "installments": rng.choice([1, 1, 1, 3, 6, 12], n_orders).astype("int32"),
        },
        out / "payments.parquet",
    )
    print(f"[gen] payments: {n_orders}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=int, default=1_000_000, help="number of orders")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--out", default="data")
    ap.add_argument("--mutate-dims", action="store_true")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed if not args.mutate_dims else args.seed + 1)
    raw = pathlib.Path(args.out) / "raw"
    raw.mkdir(parents=True, exist_ok=True)

    n_customers = max(1000, args.scale // 20)
    n_products = max(500, args.scale // 200)

    if args.mutate_dims:
        # Only regenerate dimensions (for a 2nd SCD2 snapshot); keep facts as-is.
        # Derive the row counts from the EXISTING dim files so we stay in sync
        # with whatever scale the facts were generated at.
        n_customers = pq.read_table(raw / "customers.parquet").num_rows
        n_products = pq.read_table(raw / "products.parquet").num_rows
        gen_customers(rng, n_customers, raw, mutate=True)
        gen_products(rng, n_products, raw, mutate=True)
        print(f"[gen] dims mutated for SCD2 snapshot ({n_customers} cust, {n_products} prod)")
        return

    gen_marketplaces(raw)
    gen_customers(rng, n_customers, raw, mutate=False)
    gen_products(rng, n_products, raw, mutate=False)
    gen_facts(rng, args.scale, n_customers, n_products, args.days, raw)
    print(f"[gen] done -> {raw}")


if __name__ == "__main__":
    main()
