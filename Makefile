SHELL := /bin/bash
PROJECT := $(CURDIR)
PY := .venv/bin/python
DBT := .venv/bin/dbt
export DUCKDB_PATH := $(PROJECT)/warehouse/lakehouse.duckdb
export SILVER_PATH := $(PROJECT)/data/silver

SCALE ?= 1000000
EVENTS ?= 500
RATE ?= 200
STREAM_COMPOSE := docker compose -f docker-compose.streaming.yml
SERVING_COMPOSE := docker compose -f docker-compose.serving.yml

.PHONY: help install pipeline benchmark scd2-demo airflow clean \
        stream-up stream-produce stream-consume stream-down stream-demo \
        serving-up serving-load serving-down serving-demo \
        stream-serving realtime-demo

help:
	@echo "make install     - create .venv and install the data stack"
	@echo "make pipeline    - run generate->bronze->silver->dbt->validate (SCALE=$(SCALE))"
	@echo "make benchmark   - incremental vs full-refresh timing"
	@echo "make scd2-demo   - mutate dims + re-snapshot to show SCD Type 2 history"
	@echo "make airflow     - install Airflow in .venv-airflow and launch standalone"
	@echo "make stream-demo - start Kafka, produce order events, drain to Bronze, stop"
	@echo "make serving-demo- start Postgres+Grafana, publish Gold marts (http://localhost:3000)"
	@echo "make realtime-demo- Kafka+Grafana: stream order events -> live per-minute dashboard"
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

stream-up:
	$(STREAM_COMPOSE) up -d
	@echo "waiting for Kafka to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' commerce-kafka 2>/dev/null)" = "healthy" ]; do sleep 2; done
	@echo "Kafka is up on localhost:9092"

stream-produce:
	$(PY) streaming/produce_orders.py --count $(EVENTS) --rate $(RATE)

stream-consume:
	$(PY) streaming/stream_to_bronze.py --mode batch

stream-down:
	$(STREAM_COMPOSE) down

stream-demo: stream-up stream-produce stream-consume
	@echo "Streamed events landed in data/bronze_stream/orders. Run 'make stream-down' to stop Kafka."

serving-up:
	$(SERVING_COMPOSE) up -d
	@echo "waiting for Postgres to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' commerce-postgres 2>/dev/null)" = "healthy" ]; do sleep 2; done
	@echo "Postgres up (localhost:5432), Grafana at http://localhost:3000"

serving-load:
	$(PY) serving/load_to_postgres.py

serving-down:
	$(SERVING_COMPOSE) down -v

serving-demo: serving-up serving-load
	@echo "Serving marts published. Open http://localhost:3000 (anonymous viewer) -> 'Commerce Lakehouse — Serving'."

stream-serving:
	$(PY) streaming/stream_to_serving.py --mode batch

realtime-demo: stream-up serving-up
	$(PY) streaming/produce_orders.py --count $(EVENTS) --rate $(RATE) --spread-minutes 10
	$(PY) streaming/stream_to_serving.py --mode batch
	@echo "Live per-minute aggregate in Postgres. Open http://localhost:3000 -> 'Commerce Lakehouse — Real-time orders'."

clean:
	rm -rf data warehouse dbt/target dbt/logs
