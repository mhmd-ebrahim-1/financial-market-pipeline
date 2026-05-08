from datetime import datetime, timedelta
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator


def ingestion_task():
    import sys
    sys.path.insert(0, '/opt/project')

    from pathlib import Path
    from ingestion.ingest import MarketDataIngestor
    from utils.helpers import build_project_paths, load_config

    PROJECT_ROOT = Path('/opt/project')
    config = load_config(PROJECT_ROOT / "config.yaml")
    paths = build_project_paths(config, PROJECT_ROOT)
    MarketDataIngestor(config, paths).run()


def spark_transform_task():
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
    import sys
    sys.path.insert(0, '/opt/project')

    from pathlib import Path
    from validation.quality_checks import DataQualityValidator
    from utils.helpers import build_project_paths, load_config

    PROJECT_ROOT = Path('/opt/project')
    config = load_config(PROJECT_ROOT / "config.yaml")
    paths = build_project_paths(config, PROJECT_ROOT)
    DataQualityValidator(config, paths).run()


def load_task():
    import sys
    sys.path.insert(0, '/opt/project')

    from pathlib import Path
    from loading.load import WarehouseLoader
    from utils.helpers import build_project_paths, load_config

    PROJECT_ROOT = Path('/opt/project')
    config = load_config(PROJECT_ROOT / "config.yaml")
    paths = build_project_paths(config, PROJECT_ROOT)
    WarehouseLoader(config, paths).run()


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="financial_market_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args=default_args,
) as dag:

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=ingestion_task,
    )

    transform = PythonOperator(
        task_id="spark_transform",
        python_callable=spark_transform_task,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    ingest >> transform >> validate >> load