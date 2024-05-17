# Databricks notebook source
# MAGIC %md
# MAGIC #### Overview - Run Notebooks

# COMMAND ----------

# Run notebooks

create_bronze_table = dbutils.notebook.run(path="/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/03_SCADA_data_clean_header_and_create_bronze_table", timeout_seconds=600)

clean_create_silver_table = dbutils.notebook.run(path="/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/05_SCADA_data_wrangling_and_create_silver_table", timeout_seconds=600)

create_gold_table = dbutils.notebook.run(path="/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/06_SCADA_data_structured_create_gold_table", timeout_seconds=600)

create_regression_model = dbutils.notebook.run(path="/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/07_SCADA_data_regression_model", timeout_seconds=600)

