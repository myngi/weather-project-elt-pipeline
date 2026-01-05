from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
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
    default_args=default_args,
    schedule='*/10 * * * *',  # Runs every 10 minutes
    catchup=False,
    description="Deduplicates raw FMI data and cleans nulls during schema migration"
) as dag:

    # Task to run the deduplication SQL query
    clean_data_task = BigQueryInsertJobOperator(
        task_id='run_deduplication_sql',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `data-analysis-project-478421.weather_data.cleaned_observations` AS
                    SELECT 
                        timestamp, 
                        location, 
                        MAX(source) as source,
                        MAX(latitude) as latitude,
                        MAX(longitude) as longitude,
                        MAX(temperature) as temperature,
                        MAX(humidity) as humidity,     -- Picks the actual value over null
                        MAX(wind_speed) as wind_speed   -- Picks the actual value over null
                    FROM `data-analysis-project-478421.weather_data.raw_observations`
                    GROUP BY timestamp, location
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id='google_cloud_default'
    )