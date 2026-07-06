"""Bronze layer: land each raw Parquet as-is + ingestion lineage (`_loaded_at`)
into the physical Bronze zone at data/bronze/ — an immutable, replayable landing
zone on the lake that the Spark Silver job reads directly.

- Idempotent + fresh: `COPY ... TO` overwrites each Bronze file, so re-running
  never duplicates and always reflects the current raw.
- DuckDB is used in-memory purely as the transform/writer engine here; it does
  not persist a database at this layer. The Gold layer (dbt) is what builds
  dims/facts/marts into the DuckDB warehouse.
- Batch Bronze (this) and streaming Bronze (data/bronze_stream/) are both
  append/overwrite Parquet on the same lake — one Bronze zone, two speeds.
"""
from __future__ import annotations

import argparse
import pathlib

import duckdb

ENTITIES = [
    "sales_amazon", "sales_backmarket", "settlements_oms",
    "ref_sku_map", "ref_fx", "ref_status_map", "ref_product",
]


def ingest(data_dir: str) -> None:
    raw = pathlib.Path(data_dir) / "raw"
    bronze = pathlib.Path(data_dir) / "bronze"
    bronze.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()  # in-memory: DuckDB is only the transform/writer here
    try:
        for entity in ENTITIES:
            src = raw / f"{entity}.parquet"
            if not src.exists():
                print(f"[bronze] skip {entity} (no file)")
                continue
            dst = bronze / f"{entity}.parquet"
            con.execute(
                f"COPY (SELECT *, now() AS _loaded_at FROM read_parquet('{src}')) "
                f"TO '{dst}' (FORMAT parquet)"
            )
            n = con.execute(f"SELECT count(*) FROM read_parquet('{dst}')").fetchone()[0]
            print(f"[bronze] {entity}: {n:,} rows")
    finally:
        con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default="data")
    args = ap.parse_args()
    ingest(args.data_dir)
