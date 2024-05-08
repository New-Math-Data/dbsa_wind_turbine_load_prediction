# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

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
# MAGIC #### Step 1: Download the SCADA dataset from Databricks Repo
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Access the zipped SCADA dataset from Databricks Repo
# MAGIC   2. Unzip the SCADA dataset

# COMMAND ----------

# MAGIC %sh -e
# MAGIC cd $tmpdir
# MAGIC unzip -o /Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/datasets/scada_data.csv.zip
# MAGIC ls 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Create Delta Table
# MAGIC In this step, we will:
# MAGIC   1. Access the the tempdir where we wrote the data to
# MAGIC   2. Create a permanent delta table (Bronze table for raw data)

# COMMAND ----------

file_path = "tmpdir/scada_data.csv"
df_data_wind_farm = spark.read.csv(file_path, header=True, inferSchema=True)
print('f("file_path:::{file_path}")')
df_data_wind_farm.write.format("delta").mode("overwrite").saveAsTable("scada_data")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Create Schema/Database
# MAGIC In this step, we will:
# MAGIC   1. Verify the table has been created and query the table

# COMMAND ----------

spark.sql(f"DROP DATABASE IF EXISTS {wind_turbine_load_prediction} CASCADE")
spark.sql(f"CREATE DATABASE {wind_turbine_load_prediction}")
spark.sql(f"USE {wind_turbine_load_prediction}")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM scada_data LIMIT 10
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Create Delta Table
# MAGIC In this step, we will:
# MAGIC   1. Verify the table has been created and query the table
