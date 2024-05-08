# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Wind power forecasting is essential for adequate power system stability, dispatching and cost control. Wind power is a favorable power source for green electricity generation and prediction is a crucial topic in reducing the energy lifecycle's unpredictability. Balancing energy supply and demand is critical to ensure that all energy produced from wind power is in deed actually being used. The goal of the Solution Accelerator is to provide dataset-based prediction tool for estimating wind power. Additionally, graph and tablular visulizations are provided to relay a better understanding of the wind and power production relationship and the algoristhms and methods used for forecasted predictions.
# MAGIC
# Using public SCADA datasets that include theoretical power, produced active power, wind direction, wind speed, month and hour, generated turbine power is forecasted using machine learning algorithms.
# MAGIC
# MAGIC ** Authors**
# MAGIC - Ona Niederhausern  [<rnieder@newmathdata.com>]
# MAGIC - Traey Hatch [<thatch@newmathdata.com>]
# MAGIC ___

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview - Ingest Data
# MAGIC
# MAGIC ### In this notebook you will:
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
# MAGIC %run ./config/notebook_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download the SCADA dataset from Databricks Repo
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Access the zipped SCADA dataset from Databricks Repo
# MAGIC   2. Unzip the SCADA dataset

# COMMAND ----------

%sh -e
cd $tmpdir
unzip -o /Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/datasets/scada_data.csv.zip

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Schema/Database

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

# MAGIC
# MAGIC Access the Data File

# COMMAND ----------

file_path = "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/datasets/scada_data.csv"
df_data_wind_farm = spark.read.csv(file_path, header=True, inferSchema=True)

# COMMAND ----------

# MAGIC Create a permanent delta table

# COMMAND ----------

df_data_wind_farm.write.format("delta").mode("overwrite").saveAsTable("scada_data")
# MAGIC

# MAGIC
# COMMAND ----------

# MAGIC Verify the table has been created and Query the Table
# COMMAND ----------
%sql
SELECT * FROM scada_data LIMIT 10

# MAGIC
# COMMAND ----------

