# PySpark Lakehouse (Bronze → Silver → Gold) — Banking Transactions

A local, interview‑ready **QE automation** project simulating **Azure Databricks + Delta Lake** using **PySpark**.
It generates **1,000,000 banking transactions** (Bronze), cleans them (Silver), aggregates per account (Gold),
and runs **data quality tests** with `pytest`.

## Why this is “distributed”
* All IO is **Delta Lake** (columnar, ACID).
* Partitioned by `txn_date` in Bronze for scalable loads.
* The same code runs on **Databricks** with no code change.
* Tests use Spark actions (groupBy, counts) that scale across clusters.

## Structure
    pyspark-etl/
    ├─ data/                          # generated
    ├─ etl/
    │  ├─ bronze_job.py               # generate + write Delta (partitioned)
    │  ├─ silver_job.py               # dedupe + drop nulls
    │  └─ gold_job.py                 # business aggregates
    ├─ framework/
    │  └─ spark_session.py            # SparkSession with Delta
    ├─ tests/
    │  ├─ test_bronze.py              # schema + existence
    │  ├─ test_silver.py              # no dup txn_id + no null amount
    │  └─ test_gold.py                # unique per account + totals computable
    ├─ run_local.sh
    ├─ pytest.ini
    └─ requirements.txt

## Run locally
    python -m venv venv && source venv/bin/activate
    ./run_local.sh

Or run jobs manually:
    spark-submit etl/bronze_job.py --rows 1000000
    spark-submit etl/silver_job.py
    spark-submit etl/gold_job.py
    pytest

## Interview talking points
- Implemented a **Medallion Architecture** with **PySpark + Delta Lake**.
- **Two data quality checks** per layer using **pytest**.
- **Partitioned Bronze**, **de-duplicated Silver**, **aggregated Gold**.
- Scales from laptop to **Azure Databricks** unchanged.