# Simple BigQuery dbt Project

This repository contains a basic dbt project configured to connect to Google BigQuery. It demonstrates how to define simple data transformations using SQL and dbt, and includes instructions for running the project locally and orchestrating it using Google Cloud Composer.

## Project Structure

```
.
├── models
│   └── simple_model.sql
├── profiles.yml.template  # Template for your local profiles.yml
├── dbt_project.yml
└── README.md
```

* `models/`: Contains your dbt models (SQL files that define transformations).
* `profiles.yml.template`: A template for the `profiles.yml` file, which holds database connection details. **Do not commit your actual `profiles.yml` with credentials!**
* `dbt_project.yml`: The main configuration file for your dbt project.

## Prerequisites

Before you begin, ensure you have the following:

1.  **Python 3.8 or higher:** [Install Python](https://www.python.org/downloads/)
2.  **Google Cloud SDK:** [Install gcloud CLI](https://cloud.google.com/sdk/docs/install)
3.  **dbt-bigquery adapter:** Installed via pip.
4.  **Google Cloud Project:** An active GCP project with billing enabled.
5.  **BigQuery API Enabled:** Ensure the BigQuery API is enabled in your GCP project.
6.  **BigQuery Dataset:** A BigQuery dataset where dbt can create tables/views.
7.  **Google Cloud Authentication:** Authenticate the `gcloud` CLI. `gcloud auth application-default login` is recommended for local development.
8.  **Cloud Composer Environment (for orchestration):** A running Cloud Composer environment if you plan to deploy to GCP. Ensure the Composer service account has necessary BigQuery and Cloud Storage permissions.

## Local Setup and Running

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd simple-bigquery-dbt-project
    ```
    (Replace `<repository_url>` with the actual URL of your Git repository).

2.  **Set up Python environment and install dbt:**
    It's highly recommended to use a Python virtual environment.
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows use `.venv\Scripts\activate`
    pip install dbt-bigquery
    ```

3.  **Configure your BigQuery connection (`profiles.yml`):**
    * Copy the `profiles.yml.template` file to `~/.dbt/profiles.yml`.
    * Open `~/.dbt/profiles.yml` and fill in your BigQuery connection details. A common configuration for local development using Application Default Credentials is:

        ```yaml
        simple_bigquery_dbt_project: # Profile name (matches dbt_project.yml)
          target: dev
          outputs:
            dev:
              type: bigquery
              method: oauth # Uses credentials from `gcloud auth application-default login`
              project: [YOUR_GCP_PROJECT_ID] # Replace with your GCP project ID
              dataset: [YOUR_BIGQUERY_DATASET] # Replace with your BigQuery dataset name
              threads: 4
              location: [YOUR_BIGQUERY_DATASET_LOCATION] # Optional: e.g., US, EU
        ```
    * Replace bracketed values `[YOUR_...]` with your actual GCP details.
    * If you prefer using a service account key file, change `method` to `service-account` and add the `keyfile: /path/to/your/key.json` parameter.

4.  **Test the connection:**
    Navigate to the root of your dbt project directory and run:
    ```bash
    dbt debug
    ```
    This command verifies your connection to BigQuery based on your `profiles.yml`. Look for "Connection test: OK".

5.  **Run your dbt models:**
    Execute the `dbt run` command:
    ```bash
    dbt run
    ```
    dbt will compile and execute the SQL code in `models/simple_model.sql` against your BigQuery dataset. By default, this will create a view named `simple_model`.

6.  **Explore other dbt commands:**
    * `dbt compile`: Compile dbt models into executable SQL.
    * `dbt test`: Run tests defined in your project (we don't have any in this basic example, but it's good practice).
    * `dbt docs generate`: Generate documentation for your project.
    * `dbt docs serve`: Serve the generated documentation locally in your browser.

## Cloud Composer Orchestration

This section outlines how to deploy and run this dbt project using Google Cloud Composer (Apache Airflow).

1.  **Store dbt Project in GCS:**
    Upload your entire dbt project folder to the `data` folder of your Composer environment's Cloud Storage bucket.
    ```bash
    cd path/to/your/local/dbt/project
    gsutil -m cp -r . gs://YOUR_COMPOSER_BUCKET/data/simple_bigquery_dbt_project/
    ```
    Replace `YOUR_COMPOSER_BUCKET` with your actual bucket name.

2.  **Ensure Composer Service Account Permissions:**
    Verify that the service account associated with your Composer environment has the necessary IAM roles to interact with BigQuery (`roles/bigquery.jobUser`, `roles/bigquery.dataEditor`/`Owner`) and read from the Composer bucket (`roles/storage.objectViewer` or higher).

3.  **Configure `profiles.yml` for Composer:**
    The `profiles.yml` used by dbt when running in Composer should typically use the `application-default` method, relying on the Composer service account's permissions. Ensure your `profiles.yml` (or the one you place in the GCS data folder if you modify it specifically for Composer) is configured this way.

4.  **Create Airflow DAG:**
    Create a Python file (e.g., `dbt_bigquery_run.py`) to define your Airflow DAG. This DAG will typically copy the dbt project files from GCS to a temporary location on the Airflow worker and then execute dbt commands using a `BashOperator`.

    A sample DAG structure is provided in the previous response, including steps to copy from GCS and run `dbt run` using `BashOperator`. Adapt this DAG file to your project and GCS paths.

5.  **Upload DAG to Composer:**
    Upload your Python DAG file to the `dags` folder in your Composer environment's Cloud Storage bucket.
    ```bash
    gsutil cp dbt_bigquery_run.py gs://YOUR_COMPOSER_BUCKET/dags/
    ```

6.  **Monitor in Airflow UI:**
    Access your Composer environment's Airflow webserver UI. Your DAG should appear within a few minutes. You can trigger it manually or wait for its schedule. Monitor the task logs for execution details and errors.

## Authentication Details

* **Local:** The recommended method uses `gcloud auth application-default login`. This stores credentials locally that dbt can pick up using the `method: oauth` or `method: application-default` in `profiles.yml`.
* **Composer:** dbt runs using the Composer environment's service account. By setting `method: application-default` in the `profiles.yml` used in Composer, dbt will automatically use these service account credentials to connect to BigQuery.

## Troubleshooting

* **`dbt: command not found`:** dbt is not installed in your environment, or the directory containing the `dbt` executable is not in your system's PATH. Ensure you've activated your virtual environment (if used) and that `pip install dbt-bigquery` was successful.
* **`Database Error - No such file or directory: '...'`:** dbt is configured to use a service account key file (`method: service-account`), but the file specified in `profiles.yml` at the `keyfile` path does not exist or is inaccessible. Verify the path and file presence/permissions.
* **`Database Error - Unrecognized name: ...`:** This error comes from BigQuery and means the SQL query generated by dbt references a table or column name that does not exist or is misspelled in BigQuery. Check your model's SQL (`.sql` files in the `models` directory) and compare table/column names against your BigQuery schema.

## Contributing

If this were a larger project, you would include details on:

* How to submit bug reports or feature requests.
* How to set up a development environment for contributing code.
* Coding standards and guidelines.
* Process for submitting pull requests.

## License

This project is licensed under the [MIT License](LICENSE). (You would typically include a `LICENSE` file in your repository).
