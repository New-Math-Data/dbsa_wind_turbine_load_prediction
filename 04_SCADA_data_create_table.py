# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Create Delta Table for SCADA dataset
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * Create a Schema/database for the tables to reside in permanently.
# MAGIC * Remove the downloaded SCADA Dataframe dataset from session storage.
# MAGIC * Create Delta tables for easy access and queriability.
# MAGIC * Query the table to verify creation and validitity

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a Schema/database for the tables to reside in permanently

# COMMAND ----------

# Create a Schema (aka Database) for the Table
# If the Schema already exists, we need to drop it and create new
spark.sql(f"DROP DATABASE IF EXISTS wind_turbine_load_prediction CASCADE")

# Create Schema
spark.sql(f"CREATE DATABASE wind_turbine_load_prediction")

# USE command makes sure that future code blocks are being run under the correct Schema
spark.sql(f"USE wind_turbine_load_prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create and Load data into a Delta table for analysis

# COMMAND ----------

# Create a permanent delta table (Bronze table for raw data) by converting the Spark DataFrame we just made to a Delta Table

# Access the cleaned Dataframe
file_path = f"file:///tmp/scada_data.csv"

# Register the Delta directory as a table in the metastore
df_data_wind_farm.write.format("delta").mode("overwrite").saveAsTable("scada_data")

# Lets remove the DataFrame from session storage
dbutils.fs.rm(dir=file_path, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Query the table to verify creation and validitity

# COMMAND ----------

# Verify the table has been created by quering the table
%sql
SELECT * FROM scada_data LIMIT 10

