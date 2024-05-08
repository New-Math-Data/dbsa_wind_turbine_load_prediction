# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Wind power forecasting is essential for adequate power system stability, dispatching and cost control. Wind power is a favorable power source for green electricity generation and prediction is a crucial topic in reducing the energy lifecycle's unpredictability. Balancing energy supply and demand is critical to ensure that all energy produced from wind power is in deed actually being used. The goal of the Solution Accelerator is to provide dataset-based prediction tool for estimating wind power. Additionally, graph and tablular visulizations are provided to relay a better understanding of the wind and power production relationship and the algoristhms and methods used for forecasted predictions.
# MAGIC
# MAGIC Using public SCADA datasets that include theoretical power, produced active power, wind direction, wind speed, month and hour, generated turbine power is forecasted using machine learning algorithms.
# MAGIC
# MAGIC ** Authors**
# MAGIC - Ona Niederhausern  [<rnieder@newmathdata.com>]
# MAGIC - Traey Hatch [<thatch@newmathdata.com>]

# MAGIC
# MAGIC %md
# MAGIC ## Overview - Setup
# MAGIC ### In this notebook you will:
# MAGIC * Configure Databricks Repo GIT Environment
# MAGIC * Configure the Solution Accelerator Environment
# MAGIC

# MAGIC %md
# MAGIC #### Step 1: Configure Databricks Repo GIT Environment
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Setup git repo configuration: (tutorial) https://partner-academy.databricks.com/learn/course/1266/play/7844/integrating-with-databricks-repos
# MAGIC

# MAGIC %md
# MAGIC #### Step 2: Configure the Solution Accelerator Environment
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Install python packages


# MAGIC %pip install pyspark

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


