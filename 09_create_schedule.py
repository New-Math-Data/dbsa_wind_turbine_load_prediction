# Databricks notebook source

# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Databricks schedules are used to automate the execution of jobs, notebooks, or libraries at specified intervals. These schedules are helpful for running recurring tasks, such as ETL (Extract, Transform, Load) jobs, data processing, model training, or any other data-related workflows.
# MAGIC
# MAGIC In this notebook we will set a schedule for piping in the forecasted wind data for the location of the wind farm.
# MAGIC
# MAGIC ** Authors**
# MAGIC - Ona Niederhausern  [<rnieder@newmathdata.com>]
# MAGIC - Traey Hatch [<thatch@newmathdata.com>]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Set a schedule to Extract forecasted wind data for the location of the wind farm.
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Set a schedule for the [INSERT] query

# COMMAND ----------


