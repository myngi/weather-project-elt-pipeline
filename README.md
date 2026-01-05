# FMI Weather Data ELT Pipeline

This project demonstrates an end-to-end ELT pipeline collecting real-time weather data from the Finnish Meteorological Institute (FMI).

## 🚀 Architecture Highlights
- **Orchestration**: Airflow 3.0 (utilizing the new FastAPI-based API server).
- **Data Streaming**: Kafka Producer/Consumer architecture for real-time ingestion.
- **Storage**: Google BigQuery (Raw and Cleaned observation tables).
- **Deduplication**: Automated SQL task in Airflow to ensure data quality.

## ✅ Task Completion Status
- [x] **Kafka Ingestion**: Producer fetches data from FMI; Consumer uploads to BQ.
- [x] **Airflow DAG**: Automated deduplication logic verified.
- [x] **Data Verification**: Verified row count reduction from 340 (raw) to 330 (cleaned).
- [x] **Environment**: Specialized Airflow 3.0 setup to handle high-performance DAG parsing.