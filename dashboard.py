import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json

# Streamlit Page Configuration
st.set_page_config(page_title="FMI Weather Analytics", layout="wide", page_icon="🌡️")

st.title("🌡️ FMI Real-Time Weather Analytics")
st.markdown("---")

# 1. Connection & Data Loading
if "gcp_service_account" in st.secrets:
    credentials_info = json.loads(st.secrets["gcp_service_account"])
    client = bigquery.Client.from_service_account_info(credentials_info)
else:
    client = bigquery.Client.from_service_account_json("config/gcp-service-account.json")

@st.cache_data(ttl=600)
def load_data():
    # FIX: Added humidity and wind_speed to the SELECT statement
    query = """
    SELECT 
        timestamp, 
        location AS station_name, 
        temperature,
        humidity,
        wind_speed
    FROM `data-analysis-project-478421.weather_data.cleaned_observations` 
    ORDER BY timestamp DESC
    """
    data = client.query(query).to_dataframe()
    data['timestamp'] = pd.to_datetime(data['timestamp'])
    return data

df = load_data()

# 2. Sidebar for Controls
st.sidebar.header("Dashboard Controls")

min_date = df['timestamp'].min().to_pydatetime()
max_date = df['timestamp'].max().to_pydatetime()

time_range = st.sidebar.slider(
    "Select Time Range",
    min_value=min_date,
    max_value=max_date,
    value=(min_date, max_date),
    format="DD/MM HH:mm"
)

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

# 3. Top-Level Metrics
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
if not filtered_df.empty:
    # Temperature Chart (Top)
    st.subheader("Temperature Trends (°C)")
    temp_chart = filtered_df.pivot_table(index='timestamp', columns='station_name', values='temperature')
    st.line_chart(temp_chart)

    # Humidity and Wind Charts (Side-by-Side)
    col_a, col_b = st.columns(2)
    
    with col_a:
        st.subheader("Relative Humidity (%)")
        # FIX: Used 'station_name' to match the SQL alias
        humidity_chart = filtered_df.pivot_table(index='timestamp', columns='station_name', values='humidity')
        st.line_chart(humidity_chart)

    with col_b:
        st.subheader("Wind Speed (m/s)")
        # FIX: Used 'station_name' to match the SQL alias
        wind_chart = filtered_df.pivot_table(index='timestamp', columns='station_name', values='wind_speed')
        st.line_chart(wind_chart)
else:
    st.warning("No data found for the selected filters.")

# 5. Raw Data Table
with st.expander("View Raw Processed Data"):
    st.dataframe(filtered_df, use_container_width=True)