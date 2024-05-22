# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Apply regression model to forecasted data
# MAGIC We do not need wind direction as the turbine automatically turns into the direction of wind speed

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/08_weather_data_ingest"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/07_SCADA_data_create_regression_model"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Predict wind turbine power using perviously created Random Forest Regression model

# COMMAND ----------

from pyspark.sql.functions import from_unixtime

# Forecasted Weather 
df_infer = vec_assembler.transform(df_forecasted_filtered)

# Apply trained linear regression model to make predictions
df_infer_predictions = rf_model.transform(df_infer)
display(df_infer_predictions)

