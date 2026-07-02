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
            "gold.fct_sales": "select count(*) from gold.fct_sales",
            "gold.fct_settlement": "select count(*) from gold.fct_settlement",
            "gold.mart_reconciliation": "select count(*) from gold.mart_reconciliation",
            "gold.agg_channel_daily": "select count(*) from gold.agg_channel_daily",
            "gold.dim_product": "select count(*) from gold.dim_product",
        }
        for name, sql in checks.items():
            n = con.execute(sql).fetchone()[0]
            print(f"[validate] {name}: {n:,}")
            if n == 0:
                failures.append(f"{name} empty")

        # Referential integrity: every fact channel_sk resolves to a dim.
        orphans = con.execute(
            """
            select count(*) from gold.fct_sales f
            left join gold.dim_channel d on f.channel_sk = d.channel_sk
            where d.channel_sk is null
            """
        ).fetchone()[0]
        print(f"[validate] orphan sales (bad channel_sk): {orphans}")
        if orphans:
            failures.append("fct_sales has orphan channel_sk")

        # Exactly one current version per product in the SCD2 dim.
        dupe_current = con.execute(
            """
            select count(*) from (
                select product_id from gold.dim_product
                where is_current group by product_id having count(*) > 1
            )
            """
        ).fetchone()[0]
        print(f"[validate] products with >1 current version: {dupe_current}")
        if dupe_current:
            failures.append("dim_product has multiple current versions")

        # Reconciliation: for SETTLED orders, booked should equal net + fees
        # apart from FX drift between the sale date and the payout date (same-
        # currency USD reconciles exactly; EUR shows a small, bounded FX gap).
        # Flag only gaps too large to be explained by FX timing (> 5% of booked).
        bad_recon = con.execute(
            """
            select count(*) from gold.mart_reconciliation
            where is_settled and abs(gap_usd) > 0.05 * booked_usd
            """
        ).fetchone()[0]
        print(f"[validate] settled orders with gap beyond FX tolerance: {bad_recon}")
        if bad_recon:
            failures.append("reconciliation gap beyond FX tolerance on settled orders")
    finally:
        con.close()

    if failures:
        raise SystemExit(f"[validate] FAILED: {failures}")
    print("[validate] all checks passed")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-path", required=True)
    validate(ap.parse_args().db_path)
