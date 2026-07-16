"""Check the published serving marts against Gold. Fails (non-zero) if a mart is
missing, empty, or has drifted from the warehouse, so a broken publish cannot
pass CI while still printing a plausible-looking row count.

Runs wherever the loader runs — on the host against a host warehouse, or inside
the pipeline image against the warehouse volume:

    python serving/verify_serving.py            # localhost:5432 (docker compose)
"""
from __future__ import annotations

import argparse
import os

import duckdb
from load_to_postgres import MARTS


def verify(db_path: str, pg_dsn: str) -> None:
    # Same shape as the loader: in-memory hub, warehouse and Postgres attached
    # side by side, so both counts come from one connection.
    con = duckdb.connect()
    failures = []
    try:
        con.execute(f"ATTACH '{db_path}' AS lake (READ_ONLY)")
        con.execute("INSTALL postgres")
        con.execute("LOAD postgres")
        con.execute(f"ATTACH '{pg_dsn}' AS pg (TYPE postgres, READ_ONLY)")

        for name in MARTS:
            gold = con.execute(f"SELECT count(*) FROM lake.gold.{name}").fetchone()[0]
            try:
                served = con.execute(f"SELECT count(*) FROM pg.serving.{name}").fetchone()[0]
            except duckdb.Error:
                print(f"[verify-serving] serving.{name}: MISSING")
                failures.append(f"serving.{name} was never created")
                continue

            print(f"[verify-serving] {name}: gold={gold:,} served={served:,}")
            if served == 0:
                failures.append(f"serving.{name} is empty")
            elif served != gold:
                failures.append(f"serving.{name} has {served} rows, gold has {gold}")
    finally:
        con.close()

    if failures:
        raise SystemExit(f"[verify-serving] FAILED: {failures}")
    print("[verify-serving] serving marts match Gold")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default=os.environ.get("DUCKDB_PATH", "warehouse/lakehouse.duckdb"))
    ap.add_argument(
        "--pg",
        default=os.environ.get(
            "PG_DSN", "host=localhost port=5432 dbname=serving user=grafana password=grafana"
        ),
    )
    args = ap.parse_args()
    verify(args.db_path, args.pg)
