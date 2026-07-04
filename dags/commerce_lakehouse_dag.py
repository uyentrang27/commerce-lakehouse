"""Commerce Lakehouse — end-to-end medallion pipeline orchestrated by Airflow.

    generate ─► bronze ─► silver(Spark) ─► dbt_snapshot ─► dbt_run ─► dbt_test ─► validate
     (raw)      (DuckDB)   (Parquet)        (SCD2)          (star/incr) (tests)    (DQ gate)

Engineering signals:
- **Idempotent** at every stage (bronze CREATE OR REPLACE, silver overwrite,
  dbt incremental delete+insert, marts rebuilt) → safe to retry / backfill.
- **Retries** with exponential backoff on transient failures.
- **DQ gate** (`validate`) fails the run before bad data reaches serving.
- **Env isolation**: Airflow runs in its own venv; the data stack (Spark, dbt,
  DuckDB) runs from the project venv, invoked via BashOperator — the same
  separation real deployments enforce.

Set FLAGSHIP_HOME to the commerce-lakehouse project root (defaults to the
parent of this dags/ folder).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = os.environ.get(
    "FLAGSHIP_HOME",
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
)
PY = os.path.join(PROJECT, ".venv", "bin", "python")
DBT = os.path.join(PROJECT, ".venv", "bin", "dbt")
DBT_DIR = os.path.join(PROJECT, "dbt")
DUCKDB_PATH = os.path.join(PROJECT, "warehouse", "lakehouse.duckdb")
SILVER_PATH = os.path.join(PROJECT, "data", "silver")
SCALE = os.environ.get("FLAGSHIP_SCALE", "1000000")

# Only the two vars the pipeline needs. Do NOT merge os.environ here: `env` is a
# templated field and BashOperator.template_ext=('.sh','.bash'), so any env value
# ending in .sh (e.g. VS Code sets GIT_ASKPASS=.../askpass.sh) is treated as a
# template file to load -> TemplateNotFound. append_env=True (below) merges these
# into the real process env at runtime, keeping PATH/JAVA_HOME intact.
EXTRA_ENV = {
    "DUCKDB_PATH": DUCKDB_PATH,
    "SILVER_PATH": SILVER_PATH,
}

default_args = {
    "owner": "trang",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}


def bash(task_id, cmd):
    return BashOperator(
        task_id=task_id, bash_command=cmd,
        env=EXTRA_ENV, append_env=True, cwd=PROJECT,
    )


with DAG(
    dag_id="commerce_lakehouse",
    description="Medallion lakehouse: generate -> bronze -> silver(Spark) -> gold(dbt) -> validate",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # DuckDB is single-writer
    tags=["flagship", "lakehouse", "spark", "dbt", "medallion"],
) as dag:

    generate = bash("generate", f"{PY} scripts/gen_multisource.py --scale {SCALE} --out data")
    bronze = bash("bronze", f"{PY} scripts/ingest_bronze.py --data-dir data --db-path {DUCKDB_PATH}")
    silver = bash("silver", f"{PY} spark/silver_transform.py --data-dir data")
    dbt_snapshot = bash("dbt_snapshot", f"{DBT} snapshot --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}")
    dbt_run = bash("dbt_run", f"{DBT} run --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}")
    dbt_test = bash("dbt_test", f"{DBT} test --profiles-dir {DBT_DIR} --project-dir {DBT_DIR}")
    validate = bash("validate", f"{PY} scripts/validate.py --db-path {DUCKDB_PATH}")

    generate >> bronze >> silver >> dbt_snapshot >> dbt_run >> dbt_test >> validate
