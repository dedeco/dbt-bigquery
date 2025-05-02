from __future__ import annotations

import datetime
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator # Using EmptyOperator for clarity in flow

# Define your dbt project details and GCS path
DBT_PROJECT_NAME = "rd_bigquery"  # Match the folder name you uploaded to GCS
# Your full GCS path - Ensure this path contains the root of your dbt project
GCS_DATA_PATH = f"gs://us-central1-andresousa-comp-f88b19bf-bucket/data/{DBT_PROJECT_NAME}"
# Path on the Composer worker where the project will be copied
DBT_ROOT_PATH = "/home/airflow/gcs/data/" + DBT_PROJECT_NAME

# Define your BigQuery connection details (used for profiles.yml)
# These can be passed as environment variables to the dbt command
BIGQUERY_PROJECT = "andresousa-pso-upskilling"  # Your GCP project ID
BIGQUERY_DATASET = "dbtds"  # Your BigQuery dataset name
DBT_PROFILE = "rd_bigquery" # Match the profile name in profiles.yml

# Define the location for the Python virtual environment
# It's good practice to put it inside or next to the project directory
VENV_PATH = f"{DBT_ROOT_PATH}/.venv"

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

    start = EmptyOperator(task_id='start')

    # Task to copy dbt project from GCS to the worker's local storage
    # This ensures the dbt command can find the project files
    copy_dbt_project = BashOperator(
        task_id="copy_dbt_project",
        bash_command=f"gsutil -m cp -r {GCS_DATA_PATH} /home/airflow/gcs/data/",
    )

    # Task to set up a Python virtual environment and install dbt
    # This ensures the dbt command is available
    setup_dbt_venv = BashOperator(
        task_id="setup_dbt_venv",
        bash_command=f"""
            set -e # Exit immediately if a command exits with a non-zero status.
            # Navigate to the directory where the dbt project is copied
            cd {DBT_ROOT_PATH}

            # Create a virtual environment if it doesn't exist
            if [ ! -d "{VENV_PATH}" ]; then
                python -m venv .venv
            fi

            # Activate the virtual environment and install dbt-bigquery
            source {VENV_PATH}/bin/activate
            pip install --upgrade pip
            pip install dbt-bigquery
            # You might need to install other packages if your dbt project depends on them
            # pip install -r requirements.txt
        """,
        # Use environment variables if necessary for pip, though usually not needed here
        env={
            'PIP_DISABLE_PIP_VERSION_CHECK': '1', # Optional: disable version check warnings
        }
    )

    # Task to run dbt commands within the activated virtual environment
    # We use environment variables to pass BigQuery details to dbt's profiles.yml
    dbt_run_command = f"""
        set -e # Exit immediately if a command exits with a non-zero status.
        # Navigate to the dbt project directory
        cd {DBT_ROOT_PATH}

        # Activate the virtual environment
        source {VENV_PATH}/bin/activate

        # Run the dbt command
        dbt run --profiles-dir . --target dev --profile {DBT_PROFILE}
    """

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command=dbt_run_command,
        env={ # Set environment variables for dbt's profiles.yml
            'DBT_BIGQUERY_PROJECT': BIGQUERY_PROJECT,
            'DBT_BIGQUERY_DATASET': BIGQUERY_DATASET,
            # Add other env vars if your profiles.yml uses them (e.g., for location)
            # 'BIGQUERY_LOCATION': 'US', # Example
        },
    )

    end = EmptyOperator(task_id='end')

    # Define the task dependencies
    start >> copy_dbt_project >> setup_dbt_venv >> run_dbt_models >> end