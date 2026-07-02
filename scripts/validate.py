"""Data-quality gate on the Gold marts. Fails (non-zero) if any check breaks so
a bad run never reaches the serving layer / Power BI.
"""
from __future__ import annotations

import argparse

import duckdb

def validate(db_path: str) -> None:
    con = duckdb.connect(db_path, read_only=True)
    failures = []
    try:
        checks = {
            "gold.fct_orders": "select count(*) from gold.fct_orders",
            "gold.fct_order_items": "select count(*) from gold.fct_order_items",
            "gold.agg_marketplace_daily": "select count(*) from gold.agg_marketplace_daily",
            "gold.dim_customer": "select count(*) from gold.dim_customer",
        }
        for name, sql in checks.items():
            n = con.execute(sql).fetchone()[0]
            print(f"[validate] {name}: {n:,}")
            if n == 0:
                failures.append(f"{name} empty")

        # Referential integrity: every fact marketplace_sk resolves to a dim.
        orphans = con.execute(
            """
            select count(*) from gold.fct_orders f
            left join gold.dim_marketplace d on f.marketplace_sk = d.marketplace_sk
            where d.marketplace_sk is null
            """
        ).fetchone()[0]
        print(f"[validate] orphan orders (bad marketplace_sk): {orphans}")
        if orphans:
            failures.append("fct_orders has orphan marketplace_sk")

        # Exactly one current version per customer in the SCD2 dim.
        dupe_current = con.execute(
            """
            select count(*) from (
                select customer_id from gold.dim_customer
                where is_current group by customer_id having count(*) > 1
            )
            """
        ).fetchone()[0]
        print(f"[validate] customers with >1 current version: {dupe_current}")
        if dupe_current:
            failures.append("dim_customer has multiple current versions")
    finally:
        con.close()

    if failures:
        raise SystemExit(f"[validate] FAILED: {failures}")
    print("[validate] all checks passed")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    validate(ap.parse_args().db_path)
