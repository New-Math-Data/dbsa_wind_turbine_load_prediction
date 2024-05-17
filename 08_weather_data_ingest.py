# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Ingest Streamed Pipeline Data
# MAGIC
# MAGIC * Connect VisualCrossing API Endpoint to retrieve 15 days of forcasted weather data in Yalova, Marmara Bölgesi, Turkey. Note: The weather data obtained from the VisualCrossing API is forecasted on an hourly basis, whereas our SCADA data was computed using 10-minute intervals.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's adhere to best practices by securely storing our API Key using the Databricks Secrets utility

# COMMAND ----------

# MAGIC %sh databricks secrets put --scope forecasted_weather_data --key --"5MKDL5XX9XK4RGDWC4B5MJQXE"

# COMMAND ----------

# Read secret from the secret scope
secret_value = dbutils.secrets.get(scope="forecasted_weather_data", key="key")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Retrieve the forecasted weather data for the turbine's location, Yalova, Turkey

# COMMAND ----------

import urllib.request
import sys
import json

# Ingest forecasted weather data                
try:
  # Forcasted 15 days of weather data
  # weather_results_bytes = urllib.request.urlopen("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/yalova%2C%20turkey?unitGroup=metric&include=days%2Chours&key=5MKDL5XX9XK4RGDWC4B5MJQXE&contentType=json")

  weather_results_bytes = urllib.request.urlopen("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/yalova%2C%20turkey?unitGroup=metric&include=days%2Chours&key=secret_value&contentType=json")

  # Parse the results as JSON
  forecasted_weather_data = json.load(weather_results_bytes)
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
    for hour in day["hours"]:
        forecasted_data.append({ "wind_speed_ms_hourly_avg": hour["windspeed"], "timestamp": hour["datetimeEpoch"]} )

display(forecasted_data)

df_forecasted = spark.createDataFrame(forecasted_data).select("wind_speed_ms_hourly_avg")
display(df_forecasted)

