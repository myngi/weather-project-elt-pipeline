import pandas as pd
import fmi_weather_client as fmi
import time

# Station IDs (FMISID)
# 101185: Asikkala Pulkkilanharju
# 100971: Helsinki Kaisaniemi
# 102006: Inari Saariselkä Kaunispää
# 100968: Vantaa Helsinki-Vantaan lentoasema
# 100683: Porvoo Kilpilahti satama
STATION_IDS = [101185, 100971, 102006, 100968, 100683]

print("Starting weather data extraction for selected stations...")

for fmisid in STATION_IDS:
    try:
        # Fetch the latest observation for the station
        weather = fmi.observation_by_station_id(fmisid)
        
        if weather is not None:
            message = {
                "timestamp": str(pd.to_datetime(weather.data.time)),
                "location": weather.place,
                "source": "FMI",
                "latitude": weather.lat,
                "longitude": weather.lon,
                "temperature": weather.data.temperature.value
            }
            print(f"Fetched: {message['location']} | Temperature: {message['temperature']}°C")
            
           
        
        # Short pause to avoid overwhelming the API
        time.sleep(1) 
        
    except Exception as e:
        print(f"Error fetching data for station {fmisid}: {e}")

print("Extraction cycle complete.")