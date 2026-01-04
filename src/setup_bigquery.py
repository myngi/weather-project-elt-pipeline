from google.cloud import bigquery
import os

# 1. Configuration
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "config/gcp-service-account.json"

project_id = "data-analysis-project-478421"
dataset_id = f"{project_id}.weather_data"
table_id = f"{dataset_id}.raw_observations"

client = bigquery.Client(project=project_id)

# 2. Create the Dataset
dataset = bigquery.Dataset(dataset_id)
dataset.location = "EU"  # Keeping data in the EU region

try:
    client.create_dataset(dataset, timeout=30)
    print(f"✅ Created dataset: {dataset_id}")
except Exception as e:
    print(f"ℹ️ Dataset already exists or: {e}")

# 3. Define Table Schema
# These types match your weather data dictionary
schema = [
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("location", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("temperature", "FLOAT", mode="NULLABLE"),
]

table = bigquery.Table(table_id, schema=schema)

# 4. Create the Table
try:
    client.create_table(table)
    print(f"✅ Created table: {table_id}")
except Exception as e:
    print(f"ℹ️ Table already exists or: {e}")

print("\n🚀 BigQuery setup complete. You are ready to stream data!")