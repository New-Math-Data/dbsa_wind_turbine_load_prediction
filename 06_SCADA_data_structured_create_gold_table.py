# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Create Structured Gold Table
# MAGIC Gold tables have a well-defined schema that accurately represents the data. Our data needs to adheres to predefined conventional datapoint standards.
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * Define datapoints to have a wind speed between 3 meters per second and 25 meters per second.

# COMMAND ----------

# Create DataFrame
df_scada_data_silver = spark.sql("""SELECT * FROM wind_turbine_load_prediction.scada_data_silver""")

# Show the DataFrame
display(df_scada_data_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### We wont be using `theoretical_power_curve_kwh` so lets remove it from our gold table, also let's remove defined datapoints where wind speed is less than 3 meters per second (the estimated manufacture cut-in speed) and greater than 25 meters per second.

# COMMAND ----------

from pyspark.sql.functions import col

df_scada_data_silver = df_scada_data_silver.drop("theoretical_power_curve_kwh")

df_scada_data_silver = df_scada_data_silver.drop("datetime")

df_scada_data_silver = df_scada_data_silver.filter((col("wind_speed_ms") >= 3.0) & (col("wind_speed_ms") <= 25.0))

display(df_scada_data_silver)


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col

# Make a new DataFrame to plot the graph
df = df_scada_data_silver.withColumn('wind_speed_ms', col('wind_speed_ms'))

# Plot wind speed vs. produced power
plt.figure(figsize=(20, 10))
sns.scatterplot(x='wind_speed_ms', y='lv_activepower_kw', data=df.toPandas())
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

# Overide the table and register with the new DataFrame as a Delta table in the metastore 
df_scada_data_silver.write.format("delta").mode("overwrite").saveAsTable("scada_data_gold")

print("Table 'scada_data_gold' exists.")



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm table creation with correct fields
# MAGIC SELECT * FROM wind_turbine_load_prediction.scada_data_gold;
