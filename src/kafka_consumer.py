from confluent_kafka import Consumer, KafkaError
from google.cloud import bigquery
import json
import os
import pandas as pd

# Update with your correct path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "config/gcp-service-account.json"

conf = {
    'bootstrap.servers': "127.0.0.1:9092",
    'group.id': "weather_sandbox_fix_v1", 
    'auto.offset.reset': 'earliest' 
}

consumer = Consumer(conf)
consumer.subscribe(['weather_data'])
bq_client = bigquery.Client()
table_id = "data-analysis-project-478421.weather_data.raw_observations"

records = []
print("Listening... Will batch-load to BigQuery every 20 records.")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error(): continue

        data = json.loads(msg.value().decode('utf-8'))
        records.append(data)
        print(f"Collected: {data['location']} ({len(records)}/20)")

        if len(records) >= 20:
            df = pd.DataFrame(records)
            
            # Essential: Convert 'timestamp' to datetime objects for BigQuery
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Configure the load job to allow schema updates
            job_config = bigquery.LoadJobConfig(
                schema_update_options=[
                    bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
                ],
                # WRITE_APPEND adds new data to the existing table
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            
            # Load the DataFrame with the job_config enabled
            job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)
            job.result() 
            
            print(f"✅ Successfully batch-loaded {len(records)} rows to BigQuery.")
            records = [] 

except KeyboardInterrupt:
    print("Stopped.")
finally:
    consumer.close()