# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview - Setup
# MAGIC ### In this notebook you will:
# MAGIC * Configure Databricks Repo GIT Environment
# MAGIC * Configure the Solution Accelerator Environment
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Configure Databricks Repo GIT Environment
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Setup git repo configuration: (tutorial) https://partner-academy.databricks.com/learn/course/1266/play/7844/integrating-with-databricks-repos

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Configure the Solution Accelerator Environment
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Install python packages

# COMMAND ----------

# MAGIC %pip install pyspark

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
