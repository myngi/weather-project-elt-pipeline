import requests

# FMI Open Data API endpoint for a simple weather query (Helsinki)
url = "https://opendata.fmi.fi/wfs?service=WFS&version=2.0.0&request=getFeature&storedquery_id=fmi::observations::weather::multipointcoverage&place=helsinki"

print("Fetching weather data from FMI...")
response = requests.get(url)

if response.status_code == 200:
    print("Success! Data received.")
    # In ELT, we first save this 'raw' data
    with open("data/raw/weather_helsinki.xml", "wb") as f:
        f.write(response.content)
    print("Raw data saved to data/raw/weather_helsinki.xml")
else:
    print(f"Error: {response.status_code}")
