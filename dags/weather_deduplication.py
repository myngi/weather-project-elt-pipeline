from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="weather_deduplication",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # Task to run the deduplication SQL query
    clean_data_task = BigQueryInsertJobOperator(
        task_id='run_deduplication_sql',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `data-analysis-project-478421.weather_data.cleaned_observations` AS
                    SELECT DISTINCT * FROM `data-analysis-project-478421.weather_data.raw_observations`
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )