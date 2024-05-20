# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Ingest Streamed Pipeline Data
# MAGIC
# MAGIC * Create a free user account with https://www.visualcrossing.com/ to object an api key and gain access to their api. 
# MAGIC
# MAGIC * Note: There is an option to instead run the forcasted weather example dataset in `datasets/example_forecasted_weather_data.json`
# MAGIC
# MAGIC * Connect VisualCrossing API Endpoint to retrieve 15 days of forcasted weather data in Yalova, Marmara Bölgesi, Turkey. Note: The weather data obtained from the VisualCrossing API is forecasted on an hourly basis, whereas our SCADA data was computed using 10-minute intervals.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a free user account with https://www.visualcrossing.com/ to object an api key and gain access to their api.
# MAGIC
# MAGIC ###### Note: There is an option to instead run the forcasted weather example dataset in datasets/example_forecasted_weather_data.json

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
  weather_results_json = urllib.request.urlopen("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/yalova%2C%20turkey?unitGroup=metric&include=days%2Chours&key=5MKDL5XX9XK4RGDWC4B5MJQXE&contentType=json")

  # Parse the results as JSON
  forecasted_weather_data = json.load(weather_results_json)
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

