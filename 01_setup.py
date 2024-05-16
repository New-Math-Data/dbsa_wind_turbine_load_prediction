# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Setup
# MAGIC ##### In this notebook you will:
# MAGIC * Configure Databricks Repo GIT Environment
# MAGIC * Configure the Solution Accelerator Environment
# MAGIC * About Databricks and the PySpark Session
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Configure Databricks Repo GIT Environment
# MAGIC Set up git repo configuration using the tutorial provided here: https://partner-academy.databricks.com/learn/course/1266/play/7844/integrating-with-databricks-repos

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Configure the Solution Accelerator Environment
# MAGIC All python packages needed for this Solution Accelerator are pre-installed in the Databricks environment

# COMMAND ----------

# MAGIC %md
# MAGIC ##### About Databricks and the PyDpark Session
# MAGIC
# MAGIC PySpark is pre-installed in Databricks notebook.
# MAGIC
# MAGIC The **`SparkSession`** class is the single entry point to all functionality in Spark using the DataFrame API.
# MAGIC
# MAGIC In Databricks notebooks, the SparkSession is created for you (spark = SparkSession.builder.getOrCreate()), and stored in the variable `spark`.
# MAGIC
# MAGIC In this Databricks Wind Turbine Load Prediction Solution Accelerator notebook, ee will use the `spark` object to create DataFrames, register DataFrames as tables and execute SQL queries.
# MAGIC
