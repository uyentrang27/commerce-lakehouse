"""Benchmark: incremental refresh vs full rebuild of the Gold fact tables.

It appends one new day of orders to the Silver lake, then times:
  1. `dbt run` (incremental)  — processes ONLY the new day (delete+insert).
  2. `dbt run --full-refresh` — rebuilds the whole fact history.

The gap is the point of incremental modelling: steady-state refresh cost scales
with the daily delta, not with total history. Prints real numbers you can cite.

Run AFTER a full pipeline build (so Gold already exists).
"""
from __future__ import annotations

import os
import pathlib
import subprocess
import time
from datetime import date, timedelta

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

PROJECT = pathlib.Path(__file__).resolve().parent.parent
DB = str(PROJECT / "warehouse" / "lakehouse.duckdb")
SILVER = PROJECT / "data" / "silver"
DBT_DIR = str(PROJECT / "dbt")
DBT = str(PROJECT / ".venv" / "bin" / "dbt")
ENV = {**os.environ, "DUCKDB_PATH": DB, "SILVER_PATH": str(SILVER)}


def dbt(*args) -> float:
    t = time.perf_counter()
    r = subprocess.run(
        [DBT, *args, "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR],
        env=ENV, capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(r.stdout[-2000:])
        raise SystemExit("dbt failed")
    return time.perf_counter() - t


def append_delta_day() -> tuple[str, int]:
    """Synthesize one new day of orders + items into the Silver lake."""
    con = duckdb.connect(DB, read_only=True)
    max_date = con.execute("select max(order_date) from gold.fct_orders").fetchone()[0]
    max_oid = con.execute("select max(order_id) from gold.fct_orders").fetchone()[0]
    max_iid = con.execute("select max(order_item_id) from gold.fct_order_items").fetchone()[0]
    n_cust = con.execute("select count(*) from gold.dim_customer where is_current").fetchone()[0]
    mkts = con.execute(
        "select marketplace_id, marketplace_name, channel_type, commission_rate from gold.dim_marketplace"
    ).fetchall()
    prods = con.execute(
        "select product_id, category, brand, unit_cost from gold.dim_product where is_current"
    ).fetchall()
    con.close()

    new_date = (date.fromisoformat(str(max_date)) + timedelta(days=1)).isoformat()
    rng = np.random.default_rng(7)
    n = max(1000, n_cust // 2)  # delta ~ half the customer count in orders

    oid = np.arange(max_oid + 1, max_oid + 1 + n, dtype="int64")
    mk = [mkts[i] for i in rng.integers(0, len(mkts), n)]
    mk_id = np.array([m[0] for m in mk], dtype="int32")
    mk_name = np.array([m[1] for m in mk])
    mk_chan = np.array([m[2] for m in mk])
    mk_comm = np.array([m[3] for m in mk], dtype="float64")

    # Silver orders partition (order_date encoded in the folder, not the file).
    orders_tbl = {
        "order_id": oid,
        "customer_id": rng.integers(1, n_cust + 1, n),
        "marketplace_id": mk_id,
        "marketplace_name": mk_name,
        "channel_type": mk_chan,
        "commission_rate": mk_comm,
        "order_status": np.array(["delivered"] * n),
        "is_returned": np.zeros(n, dtype=bool),
        "is_cancelled": np.zeros(n, dtype=bool),
        "currency": np.array(["VND"] * n),
    }
    part = SILVER / "orders" / f"order_date={new_date}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(orders_tbl), part / "delta.parquet")

    # Silver order_items (1-3 items per new order).
    items_per = rng.integers(1, 4, n)
    total = int(items_per.sum())
    iid = np.arange(max_iid + 1, max_iid + 1 + total, dtype="int64")
    item_oid = np.repeat(oid, items_per)
    pr = [prods[i] for i in rng.integers(0, len(prods), total)]
    pr_id = np.array([p[0] for p in pr], dtype="int64")
    pr_cat = np.array([p[1] for p in pr])
    pr_brand = np.array([p[2] for p in pr])
    pr_cost = np.array([p[3] for p in pr], dtype="float64")
    qty = rng.integers(1, 5, total).astype("int32")
    price = np.round(rng.uniform(20, 2000, total), 2)
    disc = np.round(rng.choice([0, 0, 5, 10]) / 100.0, 2) * np.ones(total)
    gross = np.round(qty * price, 2)
    net = np.round(gross * (1 - disc), 2)
    cost = np.round(qty * pr_cost, 2)
    items_tbl = {
        "order_item_id": iid, "order_id": item_oid, "product_id": pr_id,
        "category": pr_cat, "brand": pr_brand, "quantity": qty,
        "unit_price": price, "discount_pct": disc, "unit_cost": pr_cost,
        "gross_amount": gross, "net_amount": net, "cost_amount": cost,
        "margin_amount": np.round(net - cost, 2),
    }
    pq.write_table(pa.table(items_tbl), SILVER / "order_items" / "delta.parquet")
    return new_date, n


def main() -> None:
    print("=== appending one delta day to Silver ===")
    new_date, n_new = append_delta_day()
    print(f"delta: {n_new:,} new orders on {new_date}")

    fct = ["fct_orders", "fct_order_items"]
    print("\n=== INCREMENTAL run (process only the new day) ===")
    t_incr = dbt("run", "--select", *fct)

    con = duckdb.connect(DB, read_only=True)
    total_orders = con.execute("select count(*) from gold.fct_orders").fetchone()[0]
    con.close()

    print("\n=== FULL-REFRESH run (rebuild all history) ===")
    t_full = dbt("run", "--full-refresh", "--select", *fct)

    print("\n================= RESULT =================")
    print(f"fact rows (total):        {total_orders:,} orders")
    print(f"delta processed:          {n_new:,} orders (1 day)")
    print(f"incremental run:          {t_incr:6.2f}s")
    print(f"full-refresh run:         {t_full:6.2f}s")
    if t_incr > 0:
        print(f"speedup (full / incr):    {t_full / t_incr:6.1f}x")
    print("=========================================")


if __name__ == "__main__":
    main()
