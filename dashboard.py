import streamlit as st
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta

# Streamlit Page Configuration
st.set_page_config(page_title="FMI Weather Analytics", layout="wide", page_icon="🌡️")

st.title("🌡️ FMI Real-Time Weather Analytics")
st.markdown("---")

# 1. Connection & Data Loading
client = bigquery.Client.from_service_account_json("config/gcp-service-account.json")

@st.cache_data(ttl=600)
def load_data():
    query = """
    SELECT 
        timestamp, 
        location AS station_name, 
        temperature 
    FROM `data-analysis-project-478421.weather_data.cleaned_observations` 
    ORDER BY timestamp DESC
    """
    data = client.query(query).to_dataframe()
    # Ensure timestamp is in datetime format
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    return data

df = load_data()

# 2. Sidebar for Controls (The "Selection of Time")
st.sidebar.header("Dashboard Controls")

# Time Range Slider
min_date = df['timestamp'].min().to_pydatetime()
max_date = df['timestamp'].max().to_pydatetime()

# Allow user to pick a start and end time
time_range = st.sidebar.slider(
    "Select Time Range",
    min_value=min_date,
    max_value=max_date,
    value=(min_date, max_date),
    format="DD/MM HH:mm"
)

# Station Selection
all_stations = df['station_name'].unique()
selected_stations = st.sidebar.multiselect(
    "Filter Stations", 
    options=all_stations, 
    default=all_stations[:3]
)

# Filter the dataframe based on selection
filtered_df = df[
    (df['timestamp'] >= time_range[0]) & 
    (df['timestamp'] <= time_range[1]) &
    (df['station_name'].isin(selected_stations))
]

# 3. Top-Level Metrics (KPI Cards)
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Rows", len(filtered_df))
with col2:
    st.metric("Avg Temp", f"{filtered_df['temperature'].mean():.1f}°C")
with col3:
    st.metric("Max Temp", f"{filtered_df['temperature'].max()}°C")
with col4:
    st.metric("Min Temp", f"{filtered_df['temperature'].min()}°C")

st.markdown("---")

# 4. Visualizations
row2_col1, row2_col2 = st.columns([2, 1])

with row2_col1:
    st.subheader("Temperature Trends")
    if not filtered_df.empty:
        # Pivot the data so it works with st.line_chart
        chart_data = filtered_df.pivot_table(
            index='timestamp', 
            columns='station_name', 
            values='temperature'
        )
        st.line_chart(chart_data)
    else:
        st.warning("No data found for the selected filters.")

with row2_col2:
    st.subheader("Station Distribution")
    # Show a simple bar chart of records per station
    station_counts = filtered_df['station_name'].value_counts()
    st.bar_chart(station_counts)

# 5. Raw Data Table
with st.expander("View Raw Processed Data"):
    st.dataframe(filtered_df, use_container_width=True)