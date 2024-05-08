# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]
# MAGIC
# MAGIC ## Overview - Create Tables
# MAGIC
# MAGIC
# MAGIC ** Authors**
# MAGIC - Ona Niederhausern  [<rnieder@newmathdata.com>]
# MAGIC - Traey Hatch [<thatch@newmathdata.com>]
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview - Ingest Data
# MAGIC
# MAGIC ### In this notebook you will:
# MAGIC 1. Create Table of turbine SCADA data for easy access and queriability.
# MAGIC 2. Explore the dataset and relationships.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Table of turbine SCADA data for easy access and queriability.
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Create Table using Spark SQL

# COMMAND ----------

%sql
CREATE TABLE IF NOT EXISTS sacada_data
() USING CSV
OPTION {header=true}

# COMMAND ----------
# MAGIC %md
# MAGIC Verify the table has been created and Query the Table

# COMMAND ----------

%sql
SELECT * FROM scada_data LIMIT 10

# MAGIC
# COMMAND ----------


