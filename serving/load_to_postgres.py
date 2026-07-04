"""Publish the Gold serving marts from DuckDB to a Postgres serving DB.

Grafana reads Postgres natively, so the serving layer is: dbt Gold (DuckDB) ->
this loader -> Postgres `serving` schema -> Grafana dashboards. Idempotent:
each mart is replaced in full (the marts are small pre-aggregated serving
tables, not the raw facts).

    python serving/load_to_postgres.py            # localhost:5432 (docker compose)
"""
from __future__ import annotations

import argparse
import os

import duckdb

# HUGEINT has no Postgres equivalent -> cast the affected columns to BIGINT.
# The warehouse is attached read-only as `lake`; Postgres is the write target.
MARTS = {
    "agg_channel_daily": """
        SELECT channel_name, order_date, orders,
               units::BIGINT           AS units,
               booked_usd,
               returned_orders::BIGINT AS returned_orders,
               return_rate_pct
        FROM lake.gold.agg_channel_daily
    """,
    "mart_reconciliation": "SELECT * FROM lake.gold.mart_reconciliation",
}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", default=os.environ.get("DUCKDB_PATH", "warehouse/lakehouse.duckdb"))
    ap.add_argument(
        "--pg",
        default=os.environ.get(
            "PG_DSN", "host=localhost port=5432 dbname=serving user=grafana password=grafana"
        ),
    )
    args = ap.parse_args()

    # In-memory hub so we can attach the warehouse read-only AND Postgres writable.
    con = duckdb.connect()
    con.execute(f"ATTACH '{args.db_path}' AS lake (READ_ONLY)")
    con.execute("INSTALL postgres")
    con.execute("LOAD postgres")
    con.execute(f"ATTACH '{args.pg}' AS pg (TYPE postgres)")
    con.execute("CREATE SCHEMA IF NOT EXISTS pg.serving")

    for name, sql in MARTS.items():
        con.execute(f"DROP TABLE IF EXISTS pg.serving.{name}")
        con.execute(f"CREATE TABLE pg.serving.{name} AS {sql}")
        n = con.execute(f"SELECT count(*) FROM pg.serving.{name}").fetchone()[0]
        print(f"[serving] serving.{name}: {n:,} rows")

    con.close()
    print("[serving] published Gold marts -> Postgres. Grafana: http://localhost:3000")


if __name__ == "__main__":
    main()
