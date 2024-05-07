# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/...... and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/......

# COMMAND ----------

# MAGIC %md
# MAGIC <div >
# MAGIC   <img src="https://cme-solution-accelerators-images.s3-us-west-2.amazonaws.com/toxicity/solution-accelerator-logo.png"; width="50%">
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Wind power forecasting is essential for adequate power system stability, dispatching and cost control. Wind power is a favorable power source for green electricity generation and prediction is a crucial topic in reducing the energy lifecycle's unpredictability. Balancing energy supply and demand is critical to ensure that all energy produced from wind power is in deed actually being used. The goal of the Solution Accelerator is to provide dataset-based prediction tool for estimating wind power. Additionally, graph and tablular visulizations are provided to relay a better understanding of the wind and power production relationship and the algoristhms and methods used for forecasted predictions.
#
# Using public SACADA datasets that include theoretical power, produced active power, wind direction, wind speed, month and hour, generated turbine power is forecasted using machine learning algorithms.
# MAGIC
# MAGIC ** Authors**
# MAGIC - Ramona Niederhausern  [<rnieder@newmathdata.com>]
# MAGIC - Traey Hatch [<thatch@newmathdata.com>]
# MAGIC ___

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2022]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Spark-nlp|Apache-2.0 License| https://nlp.johnsnowlabs.com/license.html | https://www.johnsnowlabs.com/
# MAGIC |Kaggle|Apache-2.0 License |https://github.com/Kaggle/kaggle-api/blob/master/LICENSE|https://github.com/Kaggle/kaggle-api|
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|

# COMMAND ----------

