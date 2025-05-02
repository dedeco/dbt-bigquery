from __future__ import annotations

import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Define your dbt project details with clear paths
DBT_PROJECT_NAME = "rd_bigquery"
# Full GCS path to your dbt project
GCS_DATA_PATH = f"gs://us-central1-andresousa-comp-f88b19bf-bucket/data/{DBT_PROJECT_NAME}"
# Define the worker's data directory - this is a fixed path in Composer
WORKER_DATA_DIR = "/home/airflow/gcs/data"
# Full path to where the project will be after copying
DBT_ROOT_PATH = f"{WORKER_DATA_DIR}/{DBT_PROJECT_NAME}"

# Define your BigQuery connection details
BIGQUERY_PROJECT = "andresousa-pso-upskilling"
BIGQUERY_DATASET = "dbtds"
DBT_PROFILE = "rd_bigquery"

with DAG(
    dag_id="dbt_bigquery_run",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["dbt", "bigquery"],
    params={
        "dbt_profile": DBT_PROFILE,
        "bq_project": BIGQUERY_PROJECT,
        "bq_dataset": BIGQUERY_DATASET,
    }
) as dag:

    start = EmptyOperator(task_id='start')

    # Improved copy command that ensures the destination directory exists
    copy_dbt_project = BashOperator(
        task_id="copy_dbt_project",
        bash_command=f"""
            mkdir -p {DBT_ROOT_PATH}
            gsutil -m cp -r {GCS_DATA_PATH}/* {DBT_ROOT_PATH}/
        """,
    )

    # For Composer 1.9.1, we need to use python -m dbt instead of the dbt command directly
    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=f"""
            set -e
            
            # Verify directory exists and navigate to it
            if [ ! -d "{DBT_ROOT_PATH}" ]; then
                echo "DBT project directory not found at {DBT_ROOT_PATH}"
                exit 1
            fi
            cd {DBT_ROOT_PATH}
            
            echo "Checking installed packages..."
            pip list | grep dbt
            
            echo "Running dbt using Python module approach..."
            # Run dbt as a Python module, which should work if dbt-bigquery is installed
            python -m dbt run --profiles-dir {DBT_ROOT_PATH} --target dev --profile {DBT_PROFILE}
        """,
        env={
            'DBT_BIGQUERY_PROJECT': BIGQUERY_PROJECT,
            'DBT_BIGQUERY_DATASET': BIGQUERY_DATASET,
            # Add PYTHONPATH to ensure Python can find modules - adjust Python version if needed
            'PYTHONPATH': '/usr/local/lib/python3.7/site-packages:/home/airflow/.local/lib/python3.7/site-packages:$PYTHONPATH',
        },
    )

    end = EmptyOperator(task_id='end')

    # Define the task dependencies
    start >> copy_dbt_project >> run_dbt_models >> end
