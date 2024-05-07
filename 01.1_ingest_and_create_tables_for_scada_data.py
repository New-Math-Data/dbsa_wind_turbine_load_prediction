# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/......

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Wind power forecasting is essential for adequate power system stability, dispatching and cost control. Wind power is a favorable power source for green electricity generation and prediction is a crucial topic in reducing the energy lifecycle's unpredictability. Balancing energy supply and demand is critical to ensure that all energy produced from wind power is in deed actually being used. The goal of the Solution Accelerator is to provide dataset-based prediction tool for estimating wind power. Additionally, graph and tablular visulizations are provided to relay a better understanding of the wind and power production relationship and the algoristhms and methods used for forecasted predictions.
# MAGIC
# Using public SCADA datasets that include theoretical power, produced active power, wind direction, wind speed, month and hour, generated turbine power is forecasted using machine learning algorithms.
# MAGIC
# MAGIC ** Authors**
# MAGIC - Ramona Niederhausern  [<rnieder@newmathdata.com>]
# MAGIC - Traey Hatch [<thatch@newmathdata.com>]
# MAGIC ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC ### In this notebook you will:
# MAGIC * Configure the Environment.
# MAGIC * Download the SCADA dataset from Databricks Repo.
# MAGIC * Unzip the SCADA dataset.
# MAGIC * Create a schema/database for the tables to reside in.
# MAGIC * Move the downloaded SCADA dataset into object storage.
# MAGIC * Write the data out in `Delta` format.
# MAGIC * Create tables for easy access and queriability.
# MAGIC * Explore the dataset and relationships.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Configure the Environment
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Setup notebook configuration
# TODO: wind turbine load capacity size
# MAGIC %run ./config/notebook_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Download the SCADA dataset
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Access the zipped SCADA dataset from Databricks Repo
# MAGIC   2. Unzip the SCADA dataset

# COMMAND ----------

# MAGIC %sh -e
# MAGIC cd $tmpdir
# MAGIC unzip -o /Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Reinitiate database environment

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {wind_turbine_load_prediction} CASCADE")
spark.sql(f"CREATE DATABASE {wind_turbine_load_prediction}")
spark.sql(f"USE {wind_turbine_load_prediction}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Delta Table
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Create a permanent delta table (Bronze table for raw data)

# COMMAND ----------

# Access the Data File
# file_path = "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/"
# df_data_wind_farm = spark.read.csv(file_path, header=True, inferSchema=True)

# Create a permanent delta table
# df_data_wind_farm.write.format("delta").mode("overwrite").saveAsTable("wind_farm_turkey_2018")

# Verify the table has been created and Query the Table
# %sql
# SELECT * FROM your_table_name LIMIT 10

# COMMAND ----------
