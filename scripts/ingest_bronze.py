"""Bronze layer: land the raw Parquet into the DuckDB warehouse under a `bronze`
schema — a queryable, audited copy of exactly what was ingested.

- Idempotent: `CREATE OR REPLACE` rebuilds each bronze table from its raw file,
  so re-running never duplicates.
- Adds `_loaded_at` for ingestion lineage.

The physical raw Parquet in data/raw/ is also read directly by the Spark Silver
job — bronze here is the SQL-queryable registration of that landing zone.
"""
from __future__ import annotations

import argparse
import pathlib

import duckdb

ENTITIES = [
    "marketplaces", "customers", "products", "orders", "order_items", "payments",
]


def ingest(raw_dir: str, db_path: str) -> None:
    raw = pathlib.Path(raw_dir) / "raw"
    con = duckdb.connect(db_path)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        for entity in ENTITIES:
            src = raw / f"{entity}.parquet"
            if not src.exists():
                print(f"[bronze] skip {entity} (no file)")
                continue
            con.execute(
                f"CREATE OR REPLACE TABLE bronze.{entity} AS "
                f"SELECT *, now() AS _loaded_at FROM read_parquet('{src}')"
            )
            n = con.execute(f"SELECT count(*) FROM bronze.{entity}").fetchone()[0]
            print(f"[bronze] {entity}: {n:,} rows")
    finally:
        con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--db-path", required=True)
    args = ap.parse_args()
    ingest(args.data_dir, args.db_path)
