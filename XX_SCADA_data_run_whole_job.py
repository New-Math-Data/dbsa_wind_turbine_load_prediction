# Databricks notebook source
# MAGIC %md
# MAGIC #### Overview - Run Notebooks

# COMMAND ----------

# Run notebooks

create_bronze_table = dbutils.notebook.run(path="./03_SCADA_data_clean_header_create_bronze_table", timeout_seconds=60)

clean_create_silver_table = dbutils.notebook.run(path="./05_SCADA_data_wrangling_create_silver_table", timeout_seconds=60)

create_gold_table = dbutils.notebook.run(path="./06_SCADA_data_structuring_create_gold_table", timeout_seconds=60)

create_regression_model = dbutils.notebook.run(path="07_SCADA_data_create_regression_model", timeout_seconds=300)

