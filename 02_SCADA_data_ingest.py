# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Ingest SCADA Data - SCADA data collection and dataset analysis
# MAGIC Wind turbines have eletronic devices attached to them with SCADA software, these devices are remotely connected to the SCADA system. The SCADA system collects data about the turbine on a continuous basis. The collected data includes details such as produced power, direction and speed of wind, and date and time. 
# MAGIC
# MAGIC Predicting forecasted power of wind turbines can help wind farm's make decisions regarding produced power, consumption and management of storage-capacity.
# MAGIC
# MAGIC During the time period January 1, 2018 to December 31, 2018, the SCADA dataset holds a total of 50,530 data points. A datapoint is formed every 10 minutes of measurements of Wind Speed, Active Power (`lv_activepower_kw`), Theoretical Power and Wind Direction.
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * Access the SCADA dataset from Databricks Repo and unzip.
# MAGIC * Read the data to a DataFrame in `Delta` format for easy access and queriability.
# MAGIC * Explore the dataset and relationships for any needed future cleaning/wrangling/normalizing.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Download the SCADA dataset from Databricks Repo and unzip.

# COMMAND ----------

# MAGIC %sh -e
# MAGIC # move into the datasets directory
# MAGIC # watch out, know that this tmp directory is in your workspace, not in root tmp, we will handle this later when we read the file
# MAGIC cd /tmp
# MAGIC # Access the zipped SCADA dataset from Databricks Repo and unzip the dataset file
# MAGIC unzip -o /Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/datasets/scada_data.csv.zip
# MAGIC
# MAGIC # make sure the file was unzipped by listing the files in the datasets directory
# MAGIC ls 
# MAGIC
# MAGIC pwd
# MAGIC # run code block, you should see a `.csv` file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the SCADA data to a DataFrame in `Delta` format for easy access and queriability. Data will be in `Delta Lake`
# MAGIC

# COMMAND ----------

import os
from pathlib import Path
import platform

# Get the path of the temporary directory, this will be the `/tmp` directory in your workspace, not the systems environment
file_path = f"file:///tmp/scada_data.csv"
print(f"file_path:::{file_path}")

# Read the dataset into the session storage. In Databricks notebooks, the SparkSession is created for you, and stored in the variable `spark`.
df_wind_farm_raw = spark.read.csv(file_path, header=True, inferSchema=True, multiLine="true", escape='"')

