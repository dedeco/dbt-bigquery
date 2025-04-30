from __future__ import annotations

import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# Define your dbt project details and GCS path
DBT_PROJECT_NAME = "rd_bigquery" # Match the folder name you uploaded to GCS
GCS_DATA_PATH = f"gs://us-central1-andresousa-comp-f88b19bf-bucket/data/{DBT_PROJECT_NAME}" # Your full GCS path
DBT_ROOT_PATH = "/home/airflow/gcs/data/" + DBT_PROJECT_NAME # Path on the Composer worker

# Define your BigQuery connection details (used for profiles.yml)
# These can be passed as environment variables to the dbt command
BIGQUERY_PROJECT = "andresousa-pso-upskilling" # Your GCP project ID
BIGQUERY_DATASET = "dbtds" # Your BigQuery dataset name
DBT_PROFILE = "rd_bigquery" # Match the profile name in profiles.yml

with DAG(
    dag_id="dbt_bigquery_run",
    schedule="@daily", # Or your desired schedule (e.g., None, '0 0 * * *')
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

    # Task to copy dbt project from GCS to the worker's local storage
    # This ensures the dbt command can find the project files
    copy_dbt_project = BashOperator(
        task_id="copy_dbt_project",
        bash_command=f"gsutil -m cp -r {GCS_DATA_PATH} /home/airflow/gcs/data/",
    )

    # Task to run dbt commands
    # We use environment variables to pass BigQuery details to dbt's profiles.yml
    dbt_run_command = f"""
    cd {DBT_ROOT_PATH} &&
    dbt run --profiles-dir . --target dev --profile {DBT_PROFILE}
    """

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=dbt_run_command,
        env={ # Set environment variables for dbt's profiles.yml
            'DBT_BIGQUERY_PROJECT': BIGQUERY_PROJECT,
            'DBT_BIGQUERY_DATASET': BIGQUERY_DATASET,
            # Add other env vars if your profiles.yml uses them (e.g., for location)
        },
    )

    # Define the task dependencies
    copy_dbt_project >> run_dbt_models
