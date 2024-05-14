# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Ingest Streamed Pipeline Data
# MAGIC
# MAGIC Databricks mlflow .....
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * Connect Tomorrow.at API Endpoint to retrieve -7 days to 7 days of weather data in Yalova, Marmara Bölgesi, Turkey

# COMMAND ----------

# MAGIC %md
# MAGIC **Connect Tomorrow.at API Endpoint to retrieve -7 days to 7 days of weather data in Yalova, Turkey**
# MAGIC

# COMMAND ----------

import urllib.request
import sys
import json
                
try:
  # Forcasted 14 days of weather data
  ResultBytes = urllib.request.urlopen("https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/yalova%2C%20turkey?unitGroup=metric&include=days%2Chours&key=5MKDL5XX9XK4RGDWC4B5MJQXE&contentType=json")

  # Parse the results as JSON
  jsonData = json.load(ResultBytes)
  print(f"jsonData:::{json.dumps(jsonData, indent=3)}")
        
except urllib.error.HTTPError  as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code, ErrorInfo)
  sys.exit()
except  urllib.error.URLError as e:
  ErrorInfo= e.read().decode() 
  print('Error code: ', e.code,ErrorInfo)
  sys.exit()

# COMMAND ----------

python -m pip install requests

# COMMAND ----------

# hourly forecasts for the next 120 hours
import requests
import json

url = "https://api.tomorrow.io/v4/weather/forecast?location=yalova&apikey=IOZMradsVxtAUVc63OAjbfeo7v9WmRUp"

headers = {"accept": "application/json"}

response = requests.get(url, headers=headers)
txt = response.text
display(txt)
txt = txt.encode('utf8').decode('utf8')
display(txt)
# Convert from text to dictionary
data = json.loads(txt)
display(data)
# Watch out when you convert from a dictionary to text (response.text), specaial characters are converted to ascii code  
# jsonstring = json.dumps(data, indent=3)
# display(jsonstring)
