# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Apply regression model to forecasted data
# MAGIC We do not need wind direction as the turbine automatically turns into the direction of wind speed

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/08_weather_data_ingest"

# COMMAND ----------

# MAGIC %md
# MAGIC Filter out wind speeds that are lower than the turbine cut-in speed and higher than the turbine shut-off speed.

# COMMAND ----------

df_hourly_speeds_filtered = df_forecasted.filter(
        (df_forecasted["wind_speed_ms_hourly_avg"] >= 3) &
        (df_forecasted["wind_speed_ms_hourly_avg"] <= 25)
    )  

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/07_SCADA_data_create_regression_model"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Predict wind turbine power using created model

# COMMAND ----------

from pyspark.sql.functions import from_unixtime

# Forecasted Weather 
df_infer = vec_assembler.transform(df_hourly_speeds_filtered)

# Apply trained linear regression model to make predictions
df_infer_predictions = lr_model.transform(df_infer)
display(df_infer_predictions)


# COMMAND ----------

import matplotlib.pyplot as plt

# Extract predicted values from DataFrame
predictions = df_infer_predictions.select('prediction').rdd.map(lambda row: row[0]).collect()

# Plot the predicted values
plt.figure(figsize=(10, 6))
plt.scatter(range(len(predictions)), predictions, color='blue', label='Predictions')
plt.xlabel('wind_speed_hourly_avg')
plt.ylabel('lv_activepower_kw_hourly_sum')
plt.title('Predicted Turbine Power Output')
plt.legend()
plt.grid(True)
plt.show()

