# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - SCADA Data Ingestion and Dataset Analysis
# MAGIC
# MAGIC Wind turbines are equipped with electronic devices that are integrated with SCADA software. These devices establish remote connections to the SCADA system, enabling continuous data collection about the turbines. This data encompasses various details, including generated power, wind direction and speed, as well as date and time stamps.
# MAGIC
# MAGIC Forecasting the power output of wind turbines plays a pivotal role in assisting wind farms in decision-making processes related to power production, consumption, and storage capacity management.
# MAGIC
# MAGIC During the time period between January 1, 2018 and December 31, 2018, the SCADA dataset encompasses a total of 50,530 data points. Each data point is generated every 10 minutes and contains measurements for Wind Speed, Active Power (`lv_activepower_kw`), Theoretical Power (`theoretical_power_curve_kwh`), and Wind Direction.
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * Access the SCADA dataset from Databricks Repo and unzip.
# MAGIC * Read the data to a DataFrame in `Delta` format for easy access and queriability.

# COMMAND ----------

# MAGIC %sh -e
# MAGIC # Access the SCADA dataset from Databricks Repo and unzip
# MAGIC
# MAGIC # Note: The tmp directory is located in your workspace, not the root tmp directory. We will address this when reading the file.
# MAGIC cd /tmp
# MAGIC
# MAGIC # Navigate to the datasets directory to access the zipped SCADA dataset from the Databricks Repo and unzip the dataset file
# MAGIC unzip -o https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction/blob/main/datasets/scada_data.csv.zip
# MAGIC
# MAGIC # Verify that the file was unzipped by listing the files in the datasets directory
# MAGIC ls 
# MAGIC
# MAGIC # Display the current working directory, you should see a `.csv` file
# MAGIC pwd
# MAGIC

# COMMAND ----------

import os
from pathlib import Path
import platform

# Load the data into a DataFrame in the Delta format for seamless access and queryability

# Retrieve the path of the temporary directory, this is the `/tmp` directory within your workspace, not the system's environment
file_path = f"file:///tmp/scada_data.csv"
print(f"File Path: {file_path}")

# Read the dataset into the session storage. In Databricks notebooks, the SparkSession is automatically created and stored in the variable `spark`.
df_wind_farm_raw = spark.read.csv(file_path, header=True, inferSchema=True, multiLine="true", escape='"')

