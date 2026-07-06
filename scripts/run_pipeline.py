"""Run the full medallion pipeline once without Airflow — the same stages the
DAG orchestrates. Handy for local dev and CI.

    python scripts/run_pipeline.py --scale 1000000
"""
from __future__ import annotations

import argparse
import os
import pathlib
import subprocess
import time

PROJECT = pathlib.Path(__file__).resolve().parent.parent
PY = str(PROJECT / ".venv" / "bin" / "python")
DBT = str(PROJECT / ".venv" / "bin" / "dbt")
DBT_DIR = str(PROJECT / "dbt")
DB = str(PROJECT / "warehouse" / "lakehouse.duckdb")
SILVER = str(PROJECT / "data" / "silver")
ENV = {**os.environ, "DUCKDB_PATH": DB, "SILVER_PATH": SILVER}


def run(label, *cmd):
    print(f"\n=== {label} ===")
    t = time.perf_counter()
    r = subprocess.run(cmd, env=ENV, cwd=str(PROJECT))
    if r.returncode != 0:
        raise SystemExit(f"{label} failed")
    print(f"[{label}] {time.perf_counter() - t:.1f}s")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=int, default=1_000_000)
    scale = ap.parse_args().scale
    (PROJECT / "warehouse").mkdir(exist_ok=True)

    run("generate", PY, "scripts/gen_multisource.py", "--scale", str(scale), "--out", "data")
    run("bronze", PY, "scripts/ingest_bronze.py", "--data-dir", "data")
    run("silver", PY, "spark/silver_transform.py", "--data-dir", "data")
    run("dbt snapshot", DBT, "snapshot", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR)
    run("dbt run", DBT, "run", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR)
    run("dbt test", DBT, "test", "--profiles-dir", DBT_DIR, "--project-dir", DBT_DIR)
    run("validate", PY, "scripts/validate.py", "--db-path", DB)
    print("\n=== pipeline OK ===")


if __name__ == "__main__":
    main()
