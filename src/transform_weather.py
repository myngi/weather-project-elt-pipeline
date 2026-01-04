import pandas as pd
import xml.etree.ElementTree as ET

# Load raw XML file
xml_file = "data/raw/weather_helsinki.xml"

# Parse XML structure
tree = ET.parse(xml_file)
root = tree.getroot()

# print that .csv was loaded correctly
print(f"Successfully loaded {xml_file}")

#  Saving a placeholder CSV to processed folder
df = pd.DataFrame({"city": ["Helsinki"], "status": ["Raw Data Received"]})
df.to_csv("data/processed/weather_helsinki.csv", index=False)
print("Transformation test complete. Saved to data/processed/weather_helsinki.csv")
