import json
import time
import pandas as pd
import fmi_weather_client as fmi
from confluent_kafka import Producer

# Kafka Configuration
config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(config)

# Target stations
STATION_IDS = [101185, 100971, 102006, 100968, 100683]
TOPIC = 'weather_data'

# Callback for delivery reports
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

print(f"Starting Kafka Producer. Sending to topic: {TOPIC}")

# Continuous extraction and production loop
try:
    while True:
        for fmisid in STATION_IDS:
            try:
                weather = fmi.observation_by_station_id(fmisid)
                if weather is not None:
                    # Logic for creating the message dictionary
                    message = {
                        "timestamp": str(pd.to_datetime(weather.data.time)),
                        "location": weather.place,
                        "source": "FMI",
                        "latitude": weather.lat,
                        "longitude": weather.lon,
                        "temperature": weather.data.temperature.value,
                        "humidity": weather.data.humidity.value if hasattr(weather.data, 'humidity') else None,
                        "wind_speed": weather.data.wind_speed.value if hasattr(weather.data, 'wind_speed') else None
                    }
                    
                    # Send to Kafka
                    producer.produce(
                        TOPIC, 
                        key=str(fmisid), 
                        value=json.dumps(message), 
                        callback=delivery_report
                    )
                    # flush ensures the message is sent before continuing
                    producer.flush() 
                    
            except Exception as e:
                print(f"Error fetching station {fmisid}: {e}")
        
        print("Batch complete. Waiting 10 minutes for next FMI update...")
        time.sleep(600) 

# Graceful shutdown on interrupt
except KeyboardInterrupt:
    print("Producer stopped.")