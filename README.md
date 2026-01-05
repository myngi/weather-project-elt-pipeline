
# 🌡️ FMI Weather Data ELT Pipeline


🔗 **[Live Dashboard Link](https://myngi-weather-project-elt-pipeline-dashboard-3q7wrd.streamlit.app/)**

This project demonstrates an end-to-end ELT (Extract, Load, Transform) pipeline that collects real-time weather observations from the Finnish Meteorological Institute (FMI) and visualizes them on an interactive dashboard.

## 🏗️ Architecture
1. **Extraction**: A Python producer fetches data from the FMI Open Data API via WFS.
2. **Ingestion**: Apache Kafka handles the real-time data stream, ensuring scalability and fault tolerance.
3. **Storage**: Data is batched and loaded into **Google BigQuery** (Raw Layer).
4. **Orchestration**: **Apache Airflow 3.0** automates the data processing, deduplicating records daily/hourly.
5. **Transformation**: BigQuery **Views** dynamically filter data into station-specific datasets
6. **Visualization**: A **Streamlit** dashboard provides real-time analytics and time-series comparisons.

## 🛠️ Data Quality & Processing
To ensure high data integrity, the pipeline includes:
* **Deduplication**: SQL procedures in BigQuery (triggered by Airflow) remove duplicate entries from the raw stream.
* **Outlier Detection**: Automated filters remove "impossible" temperature readings (outside -60°C to +50°C).
* **Location Mapping**: The pipeline automatically standardizes station names for consistent visualization.

## 🚀 How to Run
1. Install dependencies: `pip install -r requirements.txt`
2. Launch Kafka & Producer/Consumer scripts in the `src/` folder.
3. Run the dashboard: `streamlit run dashboard.py`
