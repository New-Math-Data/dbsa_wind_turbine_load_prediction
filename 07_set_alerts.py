# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git and more information about this solution accelerator at https://www.databricks.com/solutions/accelerators/[INSERT]

# MAGIC %md
# MAGIC ## Overview
# MAGIC
# MAGIC Wind turbines generally start generating electricity when wind speeds reach 6 meters per second (m/s) or 13 miles per hour (mph). This is known as the "cut-in" speed. However, just because the turbine is moving, doesn't mean they are generating the highest power the generator allows, there is a range of wind speeds in which turbines operate most efficiently.
# MAGIC For most modern wind turbines, the ideal wind speeds for maximum efficiency typically range from around 12 to 55 mph (5.4 to 24.6 m/s). Wind speeds above this range can be too strong for the turbine to operate safely, so they have systems to protect themselves by feathering the blades or shutting down entirely.
# MAGIC The "cut-out" wind speed is the speed at which turbines shut down to prevent damage from excessively high winds. This usually occurs when wind speeds reach around 25 to 30 meters per second (56 to 67 mph).
# MAGIC Wind turbines do not run (or should not run) when wind speeds are below the cut-in speed or above the cut-out speed to ensure safety and optimal performance.
# MAGIC
# MAGIC In this notebook we will set an Alert to trigger when the forecasted wind speed to set to be below 6 m/s and above 55 mph.
# MAGIC
# MAGIC ** Authors**
# MAGIC - Ona Niederhausern  [<rnieder@newmathdata.com>]
# MAGIC - Traey Hatch [<thatch@newmathdata.com>]
# MAGIC ___

# COMMAND ----------


# MAGIC %md
# MAGIC ## Step 1: Set Alerts to trigger when an event in the data occurs
# MAGIC
# MAGIC In this step, we will:
# MAGIC   1. Set Alert for when Wind Speed is under 13 miles per hour
# MAGIC   1. Set Alert for when Wind Speed is over 55 miles per hour

# COMMAND ----------

