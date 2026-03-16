"""
retail_sales_etl_dag.py
-----------------------
Airflow DAG that orchestrates the Retail Sales ETL pipeline:

    extract  →  transform  →  load_dims  →  load_facts  →  summary

Schedule: daily at 06:00 UTC
"""

import logging
import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# ── Make the include/ directory importable inside Airflow ────
sys.path.insert(0, "/opt/airflow/include")

from etl_transform import extract_csv_files, transform
from etl_load import (
    get_engine,
    load_dim_customer,
    load_dim_product,
    load_dim_date,
    load_fact_sales,
    print_load_summary,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# DAG CONFIGURATION
# ─────────────────────────────────────────────────────────────
DATA_DIR = "/opt/airflow/data"
DWH_CONN_ID = "retail_dwh"          # Airflow connection (set via env var in docker-compose)
DWH_CONN_STR = os.getenv(           # fallback: read env var directly
    "AIRFLOW_CONN_RETAIL_DWH",
    "postgresql://dwh_user:dwh_pass@postgres:5432/retail_dwh",
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ─────────────────────────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────────────────────────
def task_extract(**context):
    """Read all CSV files from DATA_DIR and push raw DataFrame to XCom."""
    raw_df = extract_csv_files(DATA_DIR)
    # XCom stores JSON – serialise as records
    context["ti"].xcom_push(key="raw_rows", value=raw_df.to_json(orient="records", date_format="iso"))
    logger.info(f"Extracted {len(raw_df):,} raw rows.")


def task_transform(**context):
    """Pull raw data, clean & reshape, push transformed DataFrames to XCom."""
    import pandas as pd
    import json

    raw_json = context["ti"].xcom_pull(key="raw_rows", task_ids="extract")
    raw_df = pd.read_json(raw_json, orient="records")

    dim_customer, dim_product, dim_date, fact_sales = transform(raw_df)

    ti = context["ti"]
    ti.xcom_push(key="dim_customer", value=dim_customer.to_json(orient="records"))
    ti.xcom_push(key="dim_product",  value=dim_product.to_json(orient="records"))
    ti.xcom_push(key="dim_date",     value=dim_date.to_json(orient="records", date_format="iso"))
    ti.xcom_push(key="fact_sales",   value=fact_sales.to_json(orient="records", date_format="iso"))

    logger.info(
        f"Transform done | customers={len(dim_customer)} | "
        f"products={len(dim_product)} | dates={len(dim_date)} | "
        f"facts={len(fact_sales)}"
    )


def task_load_dimensions(**context):
    """Load dim_customer, dim_product, dim_date into the DWH."""
    import pandas as pd

    ti = context["ti"]
    dim_customer = pd.read_json(ti.xcom_pull(key="dim_customer", task_ids="transform"))
    dim_product  = pd.read_json(ti.xcom_pull(key="dim_product",  task_ids="transform"))
    dim_date     = pd.read_json(ti.xcom_pull(key="dim_date",     task_ids="transform"))

    engine = get_engine(DWH_CONN_STR)

    load_dim_customer(dim_customer, engine)
    load_dim_product(dim_product,   engine)
    load_dim_date(dim_date,         engine)

    logger.info("All dimension tables loaded.")


def task_load_facts(**context):
    """Load fact_sales into the DWH (after dimensions are committed)."""
    import pandas as pd

    ti = context["ti"]
    fact_sales = pd.read_json(ti.xcom_pull(key="fact_sales", task_ids="transform"))

    engine = get_engine(DWH_CONN_STR)
    load_fact_sales(fact_sales, engine)

    logger.info("Fact table loaded.")


def task_summary(**context):
    """Print a row-count summary for every DWH table."""
    engine = get_engine(DWH_CONN_STR)
    print_load_summary(engine)


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_sales_etl",
    description="Daily ETL: CSV → transform → PostgreSQL retail_dwh",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 6 * * *",   # 06:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["retail", "etl", "dwh"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    load_dims = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions,
    )

    load_facts = PythonOperator(
        task_id="load_facts",
        python_callable=task_load_facts,
    )

    summary = PythonOperator(
        task_id="summary",
        python_callable=task_summary,
    )

    # ── Pipeline order ────────────────────────────────────────
    extract >> transform_task >> load_dims >> load_facts >> summary
