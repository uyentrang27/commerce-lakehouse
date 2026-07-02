SHELL := /bin/bash
PROJECT := $(CURDIR)
PY := .venv/bin/python
DBT := .venv/bin/dbt
export DUCKDB_PATH := $(PROJECT)/warehouse/lakehouse.duckdb
export SILVER_PATH := $(PROJECT)/data/silver

SCALE ?= 1000000

.PHONY: help install pipeline benchmark scd2-demo airflow clean

help:
	@echo "make install     - create .venv and install the data stack"
	@echo "make pipeline    - run generate->bronze->silver->dbt->validate (SCALE=$(SCALE))"
	@echo "make benchmark   - incremental vs full-refresh timing"
	@echo "make scd2-demo   - mutate dims + re-snapshot to show SCD Type 2 history"
	@echo "make airflow     - install Airflow in .venv-airflow and launch standalone"
	@echo "make clean       - remove data/, warehouse/, dbt/target"

install:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -r requirements.txt

pipeline:
	mkdir -p warehouse
	$(PY) scripts/run_pipeline.py --scale $(SCALE)

benchmark:
	$(PY) benchmark/benchmark_incremental.py

scd2-demo:
	$(PY) scripts/gen_multisource.py --mutate-dims --out data
	$(PY) spark/silver_transform.py --data-dir data
	$(DBT) snapshot --profiles-dir dbt --project-dir dbt
	$(DBT) run --select dim_product --profiles-dir dbt --project-dir dbt
	@echo "Inspect gold.dim_product for is_current=false history rows (price/grade changes)."

airflow:
	python3 -m venv .venv-airflow
	.venv-airflow/bin/pip install --upgrade pip
	.venv-airflow/bin/pip install "apache-airflow==2.10.5" \
		--constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"
	AIRFLOW_HOME=$(PROJECT) AIRFLOW__CORE__DAGS_FOLDER=$(PROJECT)/dags \
		AIRFLOW__CORE__LOAD_EXAMPLES=False FLAGSHIP_HOME=$(PROJECT) \
		.venv-airflow/bin/airflow standalone

clean:
	rm -rf data warehouse dbt/target dbt/logs
