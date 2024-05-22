# Databricks notebook source
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
# MAGIC * Retrieve the SCADA dataset from the Databricks Respository and extract the file, saving it as a CSV file format.
# MAGIC * Read the SCADA data into a DataFrame using the ^*`Delta` format, ensuring ACID transactions and query capabilities.
# MAGIC
# MAGIC ^*Note: "Delta" refers to Delta Lake, which is an open-source storage layer that brings ACID transactions, scalable metadata handling, and data versioning to processing engines.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Retrieve the SCADA dataset from the Databricks Respository and extract the file, saving it as a CSV file format

# COMMAND ----------

# MAGIC %sh -e
# MAGIC
# MAGIC # Note: The /tmp directory is located within your workspace, not the root tmp directory. This distinction will be addressed when accessing the file.
# MAGIC
# MAGIC # Navigate to the datasets directory to access the zipped SCADA dataset located in the Databricks Repository and unzip the dataset file
# MAGIC unzip -o ./datasets/scada_data.csv.zip -d /tmp
# MAGIC
# MAGIC # Verify that the file was unzipped by listing the files in the datasets directory
# MAGIC cd /tmp
# MAGIC ls
# MAGIC
# MAGIC # Display the current working directory, you should see a `.csv` file
# MAGIC pwd
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the SCADA data into a DataFrame using the `Delta` format, ensuring ACID transactions and query capabilities.

# COMMAND ----------

import os
from pathlib import Path
import platform

# Retrieve the path of the temporary directory, which corresponds to the /tmp directory within your workspace, not the system environment.
file_path = f"file:///tmp/scada_data.csv"
print(f"File Path: {file_path}")

# Read the dataset into the session storage. In Databricks notebooks, the SparkSession is automatically instantiated and stored in the variable `spark`.
df_wind_farm_raw = spark.read.csv(file_path, header=True, inferSchema=True, multiLine="true", escape='"')
display(df_wind_farm_raw.show())
