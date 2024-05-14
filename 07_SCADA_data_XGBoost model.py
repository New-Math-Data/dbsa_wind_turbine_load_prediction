# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - XGBoost
# MAGIC
# MAGIC Gradient Boosted Trees are an excelent tool to predict produced power from the turbines. Databricks Runtime ML has Distributed XGBoost already installed.
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * Create a XGBoost model and integrate it into Spark ML pipeline
# MAGIC * Evaluate XGBoost model for accuracy 
