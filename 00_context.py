# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT PATH]

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <div >
# MAGIC <img src="./assets/NewMathDataLogo.png">
# MAGIC </div >
# MAGIC
# MAGIC <div >
# MAGIC <assets src="./assets/Logo_3x3_outlines.png">
# MAGIC </div>
# MAGIC
# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Wind power forecasting is essential for adequate power system stability, dispatching and cost control. Wind power is a favorable power source for green electricity generation and prediction is a crucial topic in reducing the energy lifecycle's unpredictability. Balancing energy supply and demand is critical to ensure that all energy produced from wind power is in deed actually being used. The goal of the Solution Accelerator is to provide dataset-based prediction tool for estimating wind power. Additionally, graph and tablular visulizations are provided to relay a better understanding of the wind and power production relationship and the algoristhms and methods used for forecasted predictions.
# MAGIC
# MAGIC Using public SCADA datasets that include theoretical power, produced active power, wind direction, wind speed, month and hour, generated turbine power is forecasted using machine learning algorithms.
# MAGIC
# MAGIC ** Authors**
# MAGIC - Ramona Niederhausern  [<rnieder@newmathdata.com>]
# MAGIC - Traey Hatch [<thatch@newmathdata.com>]
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2022]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|

# COMMAND ----------

# MAGIC %md
# MAGIC ## About This Series of Notebooks
# MAGIC  * This series of notebooks is intended to help utility companies and wind farms correctly forecast the amount of load the farm can provide the utility.
# MAGIC  * In support of this goal, we will:
# MAGIC  * Load the SCADA data provided by each wind turbine, in the farm, into our training data from Kaggle at https://www.kaggle.com/datasets/berkerisen/wind-turbine-scada-dataset.
# MAGIC  * Create a pipeline for streaming forecassted wind speed and direction in near real-time and/or on an ad-hoc basis. This pipeline can then be used for managing tables for reporting, ad hoc queries, and/or decision support.
# MAGIC  * Use Databricks mlflow for built-in Machine Learning algorithms.
# MAGIC  * Create a dashboard for monitoring the predicted supplied load.

# COMMAND ----------

# MAGIC %md
# MAGIC ## About the Data
# MAGIC ### Kaggle Dataset
# MAGIC
# MAGIC * The dataset used in this accelerator is from Kaggle (https://www.kaggle.com/datasets/berkerisen/wind-turbine-scada-dataset).
# MAGIC
# MAGIC * Further details about this dataset
# MAGIC   * Dataset title: Wind Turbine Scada Dataset
# MAGIC     ^2018 Scada Data of a Wind Turbine in Turkey^
# MAGIC   * Dataset source URL: https://www.kaggle.com/datasets/berkerisen/wind-turbine-scada-dataset
# MAGIC   * Dataset source description: BERK ERISEN added the dataset to Kaggle 5 year ago, the country Turkey provided a year, 2018, of SCADA data from a Wind Farm
# MAGIC   * Dataset license: please see dataset source URL below
# MAGIC
# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2022]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Spark-nlp|Apache-2.0 License| https://nlp.johnsnowlabs.com/license.html | https://www.johnsnowlabs.com/
# MAGIC |Kaggle|Apache-2.0 License |https://github.com/Kaggle/kaggle-api/blob/master/LICENSE|https://github.com/Kaggle|
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|
