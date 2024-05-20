# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Create Structured Gold Table
# MAGIC Gold tables have a well-defined schema that accurately represents the data. Our data needs to adheres to predefined conventional datapoint standards.
# MAGIC
# MAGIC Despite our awareness that the turbine usually doesn't operate when the wind speed is below 3 meters per second (the estimated manufacturer cut-in speed) or exceeds 25 meters per second, we'll exclude these values from the table for model fitting purposes.
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * Define datapoints to have a wind speed between 3 meters per second and 25 meters per second.

# COMMAND ----------

# Create DataFrame
df_scada_data_silver = spark.sql("""SELECT * FROM wind_turbine_load_prediction.scada_data_silver""")

# Show the DataFrame
display(df_scada_data_silver)

# COMMAND ----------

from pyspark.sql.functions import col, window, sum, avg

# Sum by hour to do the association with the forecasted weather, which is hourly

# Group data into hourly windows and calculate the sum of lv_activepower_kw
df_hourly = df_scada_data_silver.groupBy(window("datetime", "1 hour")).agg(
    sum("lv_activepower_kw").alias("lv_activepower_kw_hourly_sum"),
    avg("wind_speed_ms").alias("wind_speed_ms_hourly_avg"))

# We'll filter out data points where the wind speed is below 3 meters per second (the estimated manufacturer cut-in speed) and above 25 meters per second, which is the shutdown speed.

# If we do not filter these values, then we will get negative prediction values in times when lv_activepower_kw is zero.

df_hourly_speeds_filtered = df_hourly.filter(
        (df_hourly["wind_speed_ms_hourly_avg"] >= 3) &
        (df_hourly["wind_speed_ms_hourly_avg"] <= 25)
    )  

# Show the result
df_hourly.show(truncate=False)

display(df_hourly)


# COMMAND ----------

dbutils.data.summarize(df_hourly)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col

# Make a new DataFrame to plot the graph
df = df_hourly.withColumn('wind_speed_ms_hourly_sum', col('wind_speed_ms_hourly_avg'))

# Plot wind speed vs. produced power
plt.figure(figsize=(10, 5))
sns.scatterplot(x='wind_speed_ms_hourly_avg', y='lv_activepower_kw_hourly_sum', data=df.toPandas())
plt.title('Wind Speed vs. Produced Power')
plt.xlabel('Wind Speed (m/s)')
plt.ylabel('Produced Power (kW)')
plt.grid(True)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's create the gold table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS wind_turbine_load_prediction.scada_data_gold;

# COMMAND ----------

# Create a permanent delta table (Gold table (high quality structured data) by converting the Spark DataFrame we created eariler to a Delta Table
spark.sql(f"USE wind_turbine_load_prediction")

# While the theoretical_power_curve_kwh is useful for comparing against forecasted values, it's not required for our model

# Now that we've generated a linear datetime using epoch timestamps to address missing data, the timestamp column is no longer needed

# Now that we have lv_activepower_kw hourly, the lv_activepower_kw column is no longer needed

# Overide the table and register with the new DataFrame as a Delta table in the metastore 
df_hourly.write.format("delta").mode("overwrite").saveAsTable("scada_data_gold")

print("Table 'scada_data_gold' exists.")



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm table creation with correct fields
# MAGIC SELECT * FROM wind_turbine_load_prediction.scada_data_gold;
