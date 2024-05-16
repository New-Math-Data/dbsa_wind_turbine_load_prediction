# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Ingest Streamed Pipeline Data
# MAGIC
# MAGIC * Connect VisualCrossing API Endpoint to retrieve 15 days of forcasted weather data in Yalova, Marmara Bölgesi, Turkey. Note: The weather data obtained from the VisualCrossing API is forecasted on an hourly basis, whereas our SCADA data was computed using 10-minute intervals.

# COMMAND ----------

import urllib.request
import sys
import json

# Ingest forecasted weather data                
try:
  # Forcasted 15 days of weather data
  ResultBytes = urllib.request.urlopen("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/yalova%2C%20turkey?unitGroup=metric&include=days%2Chours&key=5MKDL5XX9XK4RGDWC4B5MJQXE&contentType=json")

  # Parse the results as JSON
  forecasted_weather_data = json.load(ResultBytes)
  print(f"jsonData:::{json.dumps(forecasted_weather_data, indent=3)}")

except urllib.error.HTTPError  as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code, ErrorInfo)
  sys.exit()
except  urllib.error.URLError as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code,ErrorInfo)
  sys.exit()

# COMMAND ----------

# Reduce weather data response for DataFrame
print(len(forecasted_weather_data["days"]))
print(forecasted_weather_data.keys())

# Access windspeed and winddir for 15 forecasted days
forecasted_data: [] = []
for day in forecasted_weather_data["days"]:
    forecasted_data.append({ "wind_direction": day["winddir"], "wind_speed_ms": day["windspeed"], "timestamp": day["datetimeEpoch"]} )

# display(forecasted_data)

df_forecasted = spark.createDataFrame(forecasted_data)
display(df_forecasted)


# COMMAND ----------


