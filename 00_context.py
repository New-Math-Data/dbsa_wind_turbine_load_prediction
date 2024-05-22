# Databricks notebook source
# MAGIC %md
# MAGIC You can find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center">
# MAGIC <img src="https://wnu.7a7.myftpupload.com/wp-content/uploads/2022/03/newmathdatalogo.png" width="400" alt="New Math Data Logo">
# MAGIC </div>
# MAGIC <br>
# MAGIC <br>
# MAGIC
# MAGIC ## Databricks Wind Turbine Load Prediction Solutions Accelerator
# MAGIC
# MAGIC
# MAGIC ### About This Series of Notebooks
# MAGIC Wind power forecasting is essential for adequate power system stability, dispatching and cost control. Wind power is a favorable power source for green electricity generation and prediction is a crucial topic in reducing the energy lifecycle's unpredictability. Balancing energy supply and demand is critical to ensure that all energy produced from wind power is in deed actually being used. 
# MAGIC
# MAGIC The goal of the Wind Turbine Load Prediction Solution Accelerator is to provide a dataset-based prediction tool for estimating wind power. Additionally, graph and tablular visulizations are provided to relay a better understanding of the wind and power production relationship and the algoristhms and methods used for forecasted predictions.
# MAGIC
# MAGIC Using a public Supervisory Control and Data Acquisition (SCADA) dataset that includes theoretical power, produced active power, wind direction, wind speed, month and hour, generated wind turbine power is predicted using machine learning algorithms.
# MAGIC
# MAGIC #### Overview
# MAGIC * This series of notebooks is intended to help utility companies and wind farms correctly forecast the amount of load a wind farm can provide a utility company.
# MAGIC
# MAGIC In support of this goal, we will:
# MAGIC
# MAGIC * Clean and filter the SCADA data provided by one of the turbine's at Yalova, Marmara Bölgesi wind farm. The dataset reflects a single wind turbine at the farm and avalible opening at Kaggle at https://www.kaggle.com/datasets/berkerisen/wind-turbine-scada-dataset.
# MAGIC
# MAGIC * Design and build a graphical tool for analyzing dataset from wind turbines acquired by the SCADA system.
# MAGIC
# MAGIC * Design and develop a machine learning based prediction model, using both Linear and Random Forest Regression, for wind energy generation utilizing past data-sets.
# MAGIC
# MAGIC * Evaluate and examine the prediction capabilities of the proposed machine learning models using statistical based calculation, performance assessment metric Root Mean-Square Error (RMSE), and the Coefficient of determination (R2) are used to compare the performance of the model.
# MAGIC
# MAGIC * Create a pipeline for streaming forecasted wind speed and direction in near real-time and also on an ad-hoc basis. This pipeline can then be used for managing tables for reporting, ad hoc queries, and decision support.
# MAGIC
# MAGIC ##### Data used in this Wind Turbine Load Prediction Solutions Accelerator
# MAGIC   * Yalova, Turkey wind farm dataset from Kaggle
# MAGIC   * Forecasted weather data from VisualCrossing API Endpoint https://www.visualcrossing.com/
# MAGIC
# MAGIC ###### Turkey wind farm dataset from Kaggle
# MAGIC Publicly available dataset gathered from Turkey's north eastern region at the Yalova, Marmara Bölgesi wind farm.
# MAGIC The SCADA system was used to collect temperature, wind speed, wind direction, produced power, and date and time for a single turbine.
# MAGIC   * The dataset used in this accelerator is from Kaggle (https://www.kaggle.com/datasets/berkerisen/  wind-turbine-scada-dataset).
# MAGIC   * Further details about this dataset
# MAGIC   * Dataset title: Wind Turbine Scada Dataset, 2018 Scada Data of a Wind Turbine in Turkey
# MAGIC   * Dataset source URL: https://www.kaggle.com/datasets/berkerisen/wind-turbine-scada-dataset
# MAGIC   * Dataset source description: BERK ERISEN added the dataset to Kaggle 5 year ago, the country Turkey provided a year, 2018, of SCADA data from a Wind Farm
# MAGIC
# MAGIC ###### Wind Data
# MAGIC   * The real-time forecasted wind speed and direction data is sourced from VisualCrossing. The VisualCrossing API provides weather forecasts for up to 15 days. This Solutions Accelerator utilizes their free plan, which includes limited features. https://www.visualcrossing.com/
# MAGIC   * An example of the response of this api endpoint can be found here in the datasets directory: datasets/example_forecasted_weather_data.json
# MAGIC
# MAGIC ##### Assumptions
# MAGIC * The turbine was not under maintenance on any given day or time.
# MAGIC
# MAGIC ##### Findings
# MAGIC * Turbines automatically adjust to face the optimal wind direction.
# MAGIC
# MAGIC **Authors**
# MAGIC - Ramona Niederhausern  [<rnieder@newmathdata.com>]
# MAGIC - Traey Hatch [<thatch@newmathdata.com>]
# MAGIC - Ryan Johnson [<rjohnson@newmathdata.com>]
# MAGIC
# MAGIC **Contact** \
# MAGIC     New Math Data, LLC \
# MAGIC     Phone: +1 (281)  817 - 6190 \
# MAGIC     Email: info@newmathdata.com \
# MAGIC     NewMathData.com
# MAGIC
# MAGIC ___

# COMMAND ----------

# MAGIC %md
# MAGIC **Understand of the incoming SCADA Wind Turnine Dataset**

# COMMAND ----------

# MAGIC %md
# MAGIC ###### SCADA Data from Yalova Wind Farm in the Northern Region of Turkey
# MAGIC | field | type | description |
# MAGIC |---|---|---|
# MAGIC | Theoretical Power Curve (KWh) | double | Theoretical maximum power output of the wind turbine under ideal conditions. | Measured in kW.
# MAGIC | LV ActivePower (kW) | double | The low voltage active power produced by a turbine refers to the actual electrical power output generated by the turbine. |
# MAGIC | Wind Direction (°) | double | Wind direction refers to the direction from which the wind is blowing. Measured in Polar degrees. |
# MAGIC | Wind Speed (m/s) | double | Wind speed is the rate at which air moves horizontally past a specific point on the Earth's surface. Measured in meters per second. |
# MAGIC | Date/Time| Datetime | ISO 8601 format for representing date and time is "YYYY-MM-DDTHH:MM:SS" where: YYYY represents the year, MM represents the month, DD represents the day, T is a separator indicating the start of the time part, HH represents the hour (in 24-hour format), MM represents the minute, SS represents the second. For example: "2024-05-08T14:30:00" represents May 8th, 2024, at 2:30 PM. |

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright Databricks, Inc. [2022]. The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC |Library Name|Library license | Library License URL | Library Source URL |
# MAGIC |---|---|---|---|
# MAGIC |Python|Python Software Foundation (PSF) |https://github.com/python/cpython/blob/master/LICENSE|https://github.com/python/cpython|
# MAGIC |Spark|Apache-2.0 License |https://github.com/apache/spark/blob/master/LICENSE|https://github.com/apache/spark|
