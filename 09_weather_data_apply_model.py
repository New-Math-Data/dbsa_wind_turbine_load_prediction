# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Apply regression model to forecasted data

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/09_weather_data_ingest"

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/07_SCADA_data_regression_model"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Predict wind turbine power using created model

# COMMAND ----------

from pyspark.sql.functions import from_unixtime


df_infer = vec_assembler.transform(df_forecasted)
# display(df_infer)

# Apply trained linear regression model to make predictions
df_infer_predictions = lr_model.transform(df_infer)
display(df_infer_predictions)

