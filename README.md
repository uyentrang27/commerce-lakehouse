# Commerce Lakehouse — end-to-end medallion data platform

A medallion lakehouse for a multi-marketplace retailer:
raw ingestion → **Bronze** → **Silver (PySpark)** → **Gold (dbt star schema)** →
serving, all orchestrated by **Apache Airflow**. It covers dimensional modelling,
SCD Type 2, incremental processing, join/partition optimisation, data-quality
gates and idempotent orchestration end to end.

```
 SOURCES            BRONZE           SILVER (Spark)        GOLD (dbt)            SERVING
 3+ marketplaces    raw landed       cleaned · conformed   star schema          Power BI
 (batch files)  ─►  in DuckDB   ─►   broadcast joins   ─►  SCD2 dims        ─►  (import model
                    + audited        partitioned Parquet   incremental facts     + Deneb visuals)
                                                           aggregation tables
        └──────────────── Apache Airflow: idempotent · retries · backfill · DQ gate ─────────────┘
```

> **Scale:** runs on **1,000,000 orders / 2,500,000 order-items** on a laptop and
> is `--scale`-configurable higher. It's a synthetic **portfolio simulation
> inspired by real multi-source retail work** — the engineering patterns are the
> point, not the specific numbers.

## The problem

A retailer selling across 6 marketplaces — each with its own commission — needs
unified, trustworthy analytics. The data is scattered across channels, product
prices and customer locations change over time, and reports have to refresh
reliably as volume grows.

## What it delivers

- **One source of truth across marketplaces** — revenue and margin net of
  per-marketplace commission, so every report agrees on the numbers.
- **Correct history (SCD2)** — price and location changes are versioned, so
  "as-of" analysis (e.g. why margin moved) stays accurate.
- **Efficient at scale** — incremental facts + pre-aggregated tables mean cheaper
  compute and faster dashboards (2.1× vs a full rebuild here).
- **Trustworthy numbers** — a data-quality gate and idempotent loads keep bad or
  duplicated data out of serving.

## How it works, layer by layer

| Layer | Tool | What it does |
|---|---|---|
| **Bronze** | DuckDB | Idempotent `CREATE OR REPLACE` load of raw Parquet + `_loaded_at` audit — a queryable, replayable landing zone. |
| **Silver** | **PySpark** | **Broadcast joins** for small dimensions (no shuffle of the big fact); **count once / reuse** cached frames; **`coalesce` file-sizing** (no small-files problem); **partition by `order_date`** for pruning. |
| **Gold** | **dbt** | **Star schema** (fact + conformed dims, surrogate keys); **SCD Type 2** snapshots (customer city + product price history); **incremental** fact models (`delete+insert`, process only the delta); **aggregation table** for fast BI; tests + exposures + lineage. |
| **Orchestration** | **Airflow** | Idempotent stages, retries with backoff, backfill-ready `@daily`, a **DQ gate** that fails the run before bad data reaches serving, and orchestrator/data-stack **env isolation**. |
| **Serving** | **Power BI** | Import data model on the Gold star schema (fast refresh) + interactive Deneb visuals. *(built separately on Windows — see Serving.)* |

## Data model (Gold star schema)
```
                 dim_date
                    │
 dim_customer ─┐    │    ┌─ dim_marketplace
 (SCD2)        └── fct_orders ──┘
 dim_product ───── fct_order_items        agg_marketplace_daily  (pre-aggregated)
 (SCD2)
```
- `fct_orders` — grain: one row per order (incremental).
- `fct_order_items` — grain: one row per (order, product) (incremental).
- `dim_customer`, `dim_product` — **SCD Type 2** (versioned history via dbt snapshots).
- `dim_marketplace`, `dim_date` — conformed dimensions.
- `agg_marketplace_daily` — daily rollup that a dashboard hits instead of the raw fact.

## Benchmark — incremental vs full rebuild
Appending one new day and refreshing the facts, measured on this machine
(1.025M orders total, 25k-order daily delta):

| Refresh | Rows processed | Wall time |
|---|---|---|
| **Incremental** (`dbt run`) | 25,000 (1 day) | **5.9 s** |
| **Full rebuild** (`--full-refresh`) | 1,025,000 (all history) | 12.3 s |

Incremental only touches the new partition. The gap **widens with history depth**
— on a multi-year fact table a full rebuild scans everything while incremental
still processes just the day. (At this scale dbt's ~3 s CLI start-up is part of
the incremental time; the SQL delta itself is sub-second.)

## Run it

### Full pipeline without the scheduler (fastest)
```bash
make install          # create .venv + install the data stack
make pipeline         # generate → bronze → silver → dbt (snapshot/run/test) → validate
make benchmark        # incremental vs full-refresh timing
make scd2-demo        # mutate dims + re-snapshot → shows SCD Type 2 history
```

### Via Airflow
```bash
make airflow          # standalone UI at http://localhost:8080
# unpause + trigger the `commerce_lakehouse` DAG
```

## Serving (Power BI)
The Gold marts (`fct_orders`, `agg_marketplace_daily`, dimensions) are the source
for a Power BI import model with an interactive Deneb (Vega) report. The `dbt`
`exposure` `powerbi_marketplace_dashboard` records that dependency in the lineage.
*(Built on Windows / Power BI Desktop; screenshots added under `docs/`.)*

## Project layout
```
commerce-lakehouse/
├── scripts/
│   ├── generate_data.py     # synthetic multi-marketplace data (vectorised)
│   ├── ingest_bronze.py     # raw → DuckDB bronze (idempotent)
│   ├── validate.py          # data-quality gate on Gold
│   └── run_pipeline.py      # run all stages without Airflow
├── spark/silver_transform.py  # PySpark clean/conform/enrich (broadcast, partition)
├── dbt/                      # Gold: staging → snapshots (SCD2) → marts (star, incremental, agg)
├── dags/commerce_lakehouse_dag.py  # Airflow orchestration
├── benchmark/benchmark_incremental.py
└── docs/                     # Power BI screenshots
```

## Scaling to production
The pattern is warehouse-portable. Swap targets without touching the DAG:
- **DuckDB → Snowflake / Databricks / BigQuery**: change `dbt/profiles.yml` and
  the Silver output sink; the medallion structure, SCD2 snapshots and incremental
  models carry over.
- **Local Spark → cluster (Databricks/EMR)**: the Silver job already uses
  broadcast joins, partitioning and file-sizing — the habits that matter at TB scale.

---
*Portfolio project. Domain and data are synthetic; the architecture mirrors
real multi-source retail data-engineering work.*
