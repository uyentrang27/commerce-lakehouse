# Commerce Lakehouse — reproducible data-stack image (PySpark + dbt + DuckDB).
# One image runs the whole batch pipeline: generate -> bronze -> silver(Spark)
# -> dbt(snapshot/run/test) -> validate. Airflow is intentionally kept in its
# own environment (see README / `make airflow`); this image is the data stack.
FROM python:3.12-slim

# PySpark needs a JRE. procps gives `ps`, which Spark shells out to.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install the data stack into a venv AT /app/.venv, because run_pipeline.py and
# the Airflow DAG both resolve the interpreter as "<project>/.venv/bin/python".
# Keeping that layout means the same code paths work on host, in CI and here.
RUN python -m venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

# The pipeline and dbt read these; keep them pointing inside the image/volumes.
ENV DUCKDB_PATH=/app/warehouse/lakehouse.duckdb \
    SILVER_PATH=/app/data/silver \
    PYTHONUNBUFFERED=1

# Default: run the full medallion pipeline at a laptop-friendly scale.
# Override at runtime, e.g. `docker compose run --rm pipeline python \
# scripts/run_pipeline.py --scale 1000000`.
CMD ["python", "scripts/run_pipeline.py", "--scale", "200000"]
