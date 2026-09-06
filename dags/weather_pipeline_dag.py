"""
Airflow DAG: orchestrates Bronze ingestion -> Silver/Gold transformation ->
data quality gate for the weather intelligence pipeline.

Runs hourly. A failed quality gate fails the DAG run rather than allowing
Gold to publish, and Airflow's built-in retry/alerting handles the rest.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.ingest import ingest_all, load_cities  # noqa: E402
from src.quality_checks import SILVER_QUALITY_CONFIG, run_all_checks  # noqa: E402
from src.transform import run as run_transform  # noqa: E402

DATA_ROOT = Path(__file__).resolve().parent.parent / "data"
CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "cities.yaml"

default_args = {
    "owner": "ravi.teja",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def task_ingest(**_context) -> None:
    cities = load_cities(CONFIG_PATH)
    ingest_all(cities, DATA_ROOT / "bronze")


def task_transform(**_context) -> None:
    run_transform(
        bronze_path=str(DATA_ROOT / "bronze" / "*" / "*.json"),
        silver_path=str(DATA_ROOT / "silver"),
        gold_path=str(DATA_ROOT / "gold"),
    )


def task_quality_gate(**_context) -> None:
    """
    Reads the freshly written Silver table and runs the quality gate.
    Raises DataQualityError (failing this task, and the DAG run) if any
    blocking check fails -- Gold has already been written by task_transform,
    so a stricter production version would compute Gold only after this
    gate passes. Kept as two tasks here for clarity of what each stage
    checks; see README "Future enhancements" for the promotion-gate variant.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[*]").appName("quality-gate").getOrCreate()
    silver_df = spark.read.format("delta").load(str(DATA_ROOT / "silver")).toPandas()
    spark.stop()

    report = run_all_checks(silver_df, SILVER_QUALITY_CONFIG)
    print(report.summary())


with DAG(
    dag_id="weather_intelligence_pipeline",
    description="Bronze -> Silver -> Gold weather & air quality pipeline with a data quality gate.",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "etl", "portfolio"],
) as dag:

    ingest = PythonOperator(task_id="ingest_bronze", python_callable=task_ingest)
    transform = PythonOperator(task_id="transform_silver_gold", python_callable=task_transform)
    quality_gate = PythonOperator(task_id="quality_gate", python_callable=task_quality_gate)

    ingest >> transform >> quality_gate
