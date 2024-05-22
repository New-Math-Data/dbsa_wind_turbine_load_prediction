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
API_KEY="5MKDL5XX9XK4RGDWC4B5MJQXE"

try:
  # Forcasted 15 days of weather data
  weather_results_json = urllib.request.urlopen(f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/yalova%2C%20turkey?unitGroup=metric&include=days%2Chours&key={API_KEY}&contentType=json")

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

# We do not need wind direction for our model
df_forecasted = spark.createDataFrame(forecasted_data).select("wind_speed_ms_hourly_avg")
display(df_forecasted)

# Filter out wind speeds that are lower than the turbine cut-in speed and higher than the turbine shut-off speed.
df_forecasted_filtered = df_forecasted.filter(
        (df_forecasted["wind_speed_ms_hourly_avg"] >= 3) &
        (df_forecasted["wind_speed_ms_hourly_avg"] <= 25)
    ) 

# COMMAND ----------

import matplotlib.pyplot as plt

# Create DataFrame with wind speed data
df_forecasted = spark.createDataFrame(forecasted_data).select("wind_speed_ms_hourly_avg")

# Convert DataFrame to Pandas DataFrame for plotting
pandas_df = df_forecasted.toPandas()

# Plot histogram
plt.figure(figsize=(10, 6))
plt.hist(pandas_df["wind_speed_ms_hourly_avg"], bins=20, color='skyblue', edgecolor='black')
plt.xlabel('Wind Speed (m/s)')
plt.ylabel('Frequency')
plt.title('Histogram of Wind Speeds')
plt.grid(True)
plt.show()

