# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Setup
# MAGIC ##### In this notebook you will:
# MAGIC * Configure Databricks Repo GIT Environment
# MAGIC * Configure the Solution Accelerator Environment
# MAGIC * About Databricks and the pyspark Session
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Configure Databricks Repo GIT Environment
# MAGIC Setup git repo configuration: (tutorial) https://partner-academy.databricks.com/learn/course/1266/play/7844/integrating-with-databricks-repos

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Configure the Solution Accelerator Environment

# COMMAND ----------

# Install needed python packages
%pip install pyspark
%pip install requests

# COMMAND ----------

# MAGIC %md
# MAGIC ##### About Databricks and the pyspark Session
# MAGIC The **`SparkSession`** class is the single entry point to all functionality in Spark using the DataFrame API.
# MAGIC
# MAGIC In Databricks notebooks, the SparkSession is created for you, and stored in the variable `spark`:
# MAGIC
# MAGIC spark = SparkSession.builder.getOrCreate()
# MAGIC
# MAGIC We will use this `spark` object through this DB Wind Turbine Load Prediction Solution Accelerator
# MAGIC
