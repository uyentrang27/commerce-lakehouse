"""Benchmark: incremental refresh vs full rebuild of the Gold sales fact.

It appends one new day of sales to the Silver lake, then times:
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
    """Synthesize one new day of sales into the Silver lake (one partition)."""
    con = duckdb.connect(DB, read_only=True)
    max_date = con.execute("select max(order_date) from gold.fct_sales").fetchone()[0]
    prods = con.execute(
        "select product_id from gold.dim_product where is_current"
    ).fetchall()
    con.close()
    prod_ids = np.array([p[0] for p in prods], dtype="int64")

    new_date = (date.fromisoformat(str(max_date)) + timedelta(days=1)).isoformat()
    rng = np.random.default_rng(7)
    n = max(1000, len(prod_ids) * 5)   # delta day of sales

    qty = rng.integers(1, 4, n).astype("int32")
    price = np.round(rng.uniform(80, 1200, n), 2)
    sales_tbl = {
        "order_id": np.array([f"DLT-{new_date}-{i}" for i in range(n)]),
        "source": np.array(["amazon"] * n),
        "product_id": prod_ids[rng.integers(0, len(prod_ids), n)],
        "quantity": qty,
        "unit_price": price,
        "canonical_status": np.array(["DELIVERED"] * n),
        "revenue_usd": np.round(qty * price, 2),
    }
    # order_date is encoded in the partition folder, not the file.
    part = SILVER / "sales" / f"order_date={new_date}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table(sales_tbl), part / "delta.parquet")
    return new_date, n


def main() -> None:
    print("=== appending one delta day to Silver ===")
    new_date, n_new = append_delta_day()
    print(f"delta: {n_new:,} new sales on {new_date}")

    print("\n=== INCREMENTAL run (process only the new day) ===")
    t_incr = dbt("run", "--select", "fct_sales")

    con = duckdb.connect(DB, read_only=True)
    total = con.execute("select count(*) from gold.fct_sales").fetchone()[0]
    con.close()

    print("\n=== FULL-REFRESH run (rebuild all history) ===")
    t_full = dbt("run", "--full-refresh", "--select", "fct_sales")

    print("\n================= RESULT =================")
    print(f"fact rows (total):        {total:,} sales")
    print(f"delta processed:          {n_new:,} sales (1 day)")
    print(f"incremental run:          {t_incr:6.2f}s")
    print(f"full-refresh run:         {t_full:6.2f}s")
    if t_incr > 0:
        print(f"speedup (full / incr):    {t_full / t_incr:6.1f}x")
    print("=========================================")


if __name__ == "__main__":
    main()
