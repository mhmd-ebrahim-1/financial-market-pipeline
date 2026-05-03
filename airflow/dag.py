"""
dag.py
======
Airflow DAG للـ financial market pipeline.
يشغّل الـ pipeline كل يوم 10pm (الأحد - الخميس).

Pipeline:
    ingest → spark_transform → validate → load
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
import sys
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────────────────────
# Project Path - داخل الـ container بيكون /opt/project
# ─────────────────────────────────────────
PROJECT_ROOT = Path('/opt/project')
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from ingestion.ingest import MarketDataIngestor        # noqa: E402
from loading.load import WarehouseLoader               # noqa: E402
from validation.quality_checks import DataQualityValidator  # noqa: E402
from utils.helpers import build_project_paths, load_config  # noqa: E402

CONFIG_PATH = PROJECT_ROOT / "config.yaml"

# ─────────────────────────────────────────
# Helper
# ─────────────────────────────────────────
def _load_context():
    config = load_config(CONFIG_PATH)
    paths = build_project_paths(config, PROJECT_ROOT)
    return config, paths


# ─────────────────────────────────────────
# Tasks
# ─────────────────────────────────────────
def ingestion_task():
    """جلب البيانات من Yahoo Finance وحفظها في HDFS raw layer."""
    config, paths = _load_context()
    MarketDataIngestor(config, paths).run()


def spark_transform_task():
    """
    تشغيل spark_transform.py داخل spark-master container بـ spark-submit.
    ده بيستبدل الـ Pandas transform بـ PySpark حقيقي على HDFS.
    """
    result = subprocess.run(
        [
            "docker", "exec", "spark-master",
            "/opt/spark/bin/spark-submit",
            "--master", "local[*]",
            "/opt/project/processing/spark_transform.py"
        ],
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"spark-submit failed with return code {result.returncode}")

    print(">>> Spark transform completed successfully.")


def validate_task():
    """التحقق من جودة البيانات بعد الـ transform."""
    config, paths = _load_context()
    DataQualityValidator(config, paths).run()


def load_task():
    """تحميل البيانات للـ warehouse النهائي."""
    config, paths = _load_context()
    WarehouseLoader(config, paths).run()


# ─────────────────────────────────────────
# DAG Definition
# ─────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="financial_market_pipeline",
    default_args=default_args,
    description="Daily financial market ETL/ELT pipeline - Hadoop + Spark",
    schedule="0 22 * * 0-4",   # كل يوم 10pm الأحد للخميس
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["finance", "etl", "spark", "hdfs"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_market_data",
        python_callable=ingestion_task,
    )

    transform = PythonOperator(
        task_id="spark_transform",
        python_callable=spark_transform_task,
    )

    validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_task,
    )

    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=load_task,
    )

    # Pipeline order
    ingest >> transform >> validate >> load
