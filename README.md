# Commerce Lakehouse — end-to-end medallion data platform

A medallion lakehouse for a multi-channel reseller: raw ingestion → **Bronze** →
**Silver (PySpark)** → **Gold (dbt star schema)** → serving, orchestrated by
**Apache Airflow**. Its centre of gravity is the hard part of real commerce data:
**conforming several sources that speak different languages**, and **reconciling
booked sales revenue against the cash actually settled**. It covers dimensional
modelling, SCD Type 2, incremental processing, join/partition optimisation,
data-quality gates and idempotent orchestration end to end.

```
 SOURCES                       BRONZE        SILVER (Spark)          GOLD (dbt)              SERVING
 2 sales channels (different   raw landed    conform → UNION sales   star schema            Power BI
   schema/status/ccy/date)     in DuckDB     conform settlement      SCD2 dim_product        (import model
 1 settlement system        ─► + audited  ─► (separate grain)     ─► fct_sales/settlement ─► + Deneb visuals)
 + reference tables                          broadcast joins, FX     mart_reconciliation
   (sku map, FX, status)                     partitioned Parquet     agg tables
        └───────────────── Apache Airflow: idempotent · retries · backfill · DQ gate ──────────────┘
```

> **Scale:** runs on **1,000,000 sales** (≈78% one channel — a deliberate skew)
> plus **~740k settlements** on a laptop, `--scale`-configurable higher. It is a
> synthetic **portfolio simulation of a common industry pattern** — the
> engineering is the point, not the specific numbers or any real company's system.

## The problem

A refurbished-phone reseller sells through several online marketplaces and
settles cash through an order-management system. Nothing lines up out of the box:

- Each **sales channel** speaks its own language — different column names, order
  **status codes** (`"Shipped"` vs `3`), **currency** (USD vs EUR) and **date
  formats** (`2026/06/29` vs `29-06-2026`), and its own **product codes**.
- The **settlement** feed is a *different grain* — one row per cash remittance,
  net of marketplace and shipping **fees** — so it can't just be stacked onto
  sales.
- Finance needs to know not only *what was sold* but *how much cash actually came
  back*, and why the two differ (fees, FX, orders not yet paid out).

## What it delivers

- **One conformed sales table across channels** — codes mapped to a canonical
  product id, statuses standardised, every amount converted to USD, then unioned,
  so every report agrees on the numbers.
- **Sales-vs-cash reconciliation** — `mart_reconciliation` ties booked revenue to
  settled cash per order, exposing **fees**, **FX drift** between sale and payout,
  and **revenue booked but not yet collected**.
- **Correct history (SCD2)** — product price and refurbished grade changes are
  versioned, so "as-of" analysis stays accurate.
- **Efficient at scale** — incremental facts + pre-aggregated tables mean cheaper
  compute and faster dashboards.
- **Trustworthy numbers** — a data-quality gate and idempotent loads keep bad or
  duplicated data out of serving.

## How it works, layer by layer

| Layer | Tool | What it does |
|---|---|---|
| **Bronze** | DuckDB | Idempotent `CREATE OR REPLACE` load of every raw file + `_loaded_at` audit — a queryable, replayable landing zone. |
| **Silver** | **PySpark** | **Conform** each source to one schema (rename, cast messy dates, map codes); **broadcast joins** against the tiny reference tables — sku map, status map, FX — so the million-row fact never shuffles; **FX to USD**; **`unionByName`** the two conformed sales channels; keep settlement separate (different grain); **cache** the reused frame; **`coalesce`** file-sizing; **partition by `order_date`** for pruning. |
| **Gold** | **dbt** | **Star schema** (facts + conformed dims, surrogate keys); **SCD Type 2** snapshot (product price + grade history); **incremental** facts (`delete+insert`, only the delta); **`mart_reconciliation`** (booked vs settled); **aggregation table** for fast BI; tests + exposure + lineage. |
| **Orchestration** | **Airflow** | Idempotent stages, retries with backoff, backfill-ready `@daily`, a **DQ gate** that fails the run before bad data reaches serving, and orchestrator/data-stack **env isolation**. |
| **Serving** | **Power BI** | Import model on the Gold marts (fast refresh) + interactive Deneb visuals. *(built separately on Windows — see Serving.)* |

## Why joins happen at Silver (conform) vs Gold (model)

- **Silver joins to *conform*** — map each channel's product code to a canonical
  `product_id`, translate status codes, convert currency. Without this the sources
  can't be compared, let alone unioned.
- **Silver keeps different *grains* apart** — sales (one row per order) and
  settlement (one row per cash remittance) are **not** unioned; they are two
  clean tables.
- **Gold joins to *model*** — the star schema (surrogate keys, SCD2) and the
  order-level reconciliation between the two facts.

## Data model (Gold star schema)
```
                  dim_date
                     │
 dim_channel ──┬── fct_sales ──── dim_product (SCD2)
               │        │
               └── fct_settlement
                        │
                mart_reconciliation   (fct_sales ⋈ fct_settlement by order)
                agg_channel_daily     (pre-aggregated serving table)
```
- `fct_sales` — grain: one row per sold order (incremental), conformed from both channels.
- `fct_settlement` — grain: one row per cash remittance (incremental).
- `dim_product` — **SCD Type 2**: price + grade history via a dbt snapshot.
- `dim_channel`, `dim_date` — conformed dimensions.
- `mart_reconciliation` — order-grain booked-vs-settled: fees, FX drift, uncollected cash.
- `agg_channel_daily` — daily rollup a dashboard hits instead of the raw fact.

## Benchmark — incremental vs full rebuild
Appending one new day and refreshing `fct_sales`, measured on this machine
(1.02M sales total, 20k-order daily delta):

| Refresh | Rows processed | Wall time |
|---|---|---|
| **Incremental** (`dbt run`) | 20,000 (1 day) | **13.2 s** |
| **Full rebuild** (`--full-refresh`) | 1,020,000 (all history) | 17.5 s |

Incremental only touches the new partition. The gap **widens with history depth**
— on a multi-year fact a full rebuild scans everything while incremental still
processes just the day. (At this scale dbt's ~3 s CLI start-up is a large part of
the incremental time; the SQL delta itself is sub-second.)

## Run it

### Full pipeline without the scheduler (fastest)
```bash
make install          # create .venv + install the data stack
make pipeline         # generate → bronze → silver → dbt (snapshot/run/test) → validate
make benchmark        # incremental vs full-refresh timing
make scd2-demo        # mutate the product master + re-snapshot → SCD Type 2 history
```

### Via Airflow
```bash
make airflow          # standalone UI at http://localhost:8080
# unpause + trigger the `commerce_lakehouse` DAG
```

## Serving (Power BI)
The Gold marts (`agg_channel_daily`, `mart_reconciliation`, dimensions) are the
source for a Power BI import model with an interactive Deneb (Vega) report. The
`dbt` `exposure` `powerbi_reconciliation_dashboard` records that dependency in the
lineage. *(Built on Windows / Power BI Desktop; screenshots under `docs/`.)*

## Project layout
```
commerce-lakehouse/
├── scripts/
│   ├── gen_multisource.py    # synthetic multi-source data (vectorised, skewed)
│   ├── ingest_bronze.py      # raw → DuckDB bronze (idempotent)
│   ├── validate.py           # data-quality gate on Gold
│   └── run_pipeline.py       # run all stages without Airflow
├── spark/silver_transform.py # PySpark conform + union + settlement (broadcast, FX, partition)
├── dbt/                       # Gold: staging → snapshot (SCD2) → marts (star, incremental, reconciliation)
├── dags/commerce_lakehouse_dag.py  # Airflow orchestration
├── benchmark/benchmark_incremental.py
└── docs/                      # Power BI screenshots
```

## Scaling to production
The pattern is warehouse-portable. Swap targets without touching the DAG:
- **DuckDB → Snowflake / Databricks / BigQuery**: change `dbt/profiles.yml` and
  the Silver output sink; the medallion structure, conform logic, SCD2 snapshot
  and incremental models carry over.
- **Local Spark → cluster (Databricks/EMR)**: the Silver job already uses
  broadcast joins, partitioning and file-sizing — the habits that matter at TB
  scale — and the skewed channel is a built-in data-skew talking point.

---
*Portfolio project. Domain and data are synthetic; the architecture simulates a
common multi-source retail data-engineering pattern, not any specific company's
system.*
