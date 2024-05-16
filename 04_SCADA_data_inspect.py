# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Data Inspection
# MAGIC
# MAGIC ##### In this notebook we will:
# MAGIC   * Look for zero, missing or null values in the data that will cause outliers or skew the data
# MAGIC   * Explore dataset based on `summarize` statistics
# MAGIC   * Identify outliers in the dataset

# COMMAND ----------

# Create a temporary DataFrame for data cleaning purposes
df_wind_farm_bronze = spark.sql("""SELECT * FROM wind_turbine_load_prediction.scada_data_bronze""")

# Show the DataFrame
display(df_wind_farm_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Look for zero, missing or null values in the data that will cause outliers or skew the data
# MAGIC
# MAGIC Let's create a Box plot and a Histogram visulization to start to identify if we have data outliers.
# MAGIC
# MAGIC Two options:
# MAGIC
# MAGIC * Box plot: count, mean, stddev, min, max
# MAGIC summary: describe + interquartile range (IQR)
# MAGIC
# MAGIC * Histograms: display the distribution of a single variable and can help identify regions of the data where values are sparsely populated, indicating potential outliers.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display a Box plot and look for outliers for low voltage active power (lv_activepower_kw)

# COMMAND ----------

from pyspark.sql.functions import min as spark_min, max as spark_max, expr

# Calculate boxplot statistics
boxplot_stats = spark.table("wind_turbine_load_prediction.scada_data_bronze") \
    .select(
        spark_min("lv_activepower_kw").alias("min_value"),
        spark_max("lv_activepower_kw").alias("max_value"),
        expr("percentile_approx(lv_activepower_kw, 0.25)").alias("q1"),
        expr("percentile_approx(lv_activepower_kw, 0.50)").alias("q2/median"),
        expr("percentile_approx(lv_activepower_kw, 0.75)").alias("q3")
    )

# Show the result
boxplot_stats.show()


# COMMAND ----------

# MAGIC %md
# MAGIC It's worth noting that the maximum wind speed recorded is 25.21 meters per second, which coincides with the maximum safe operating speed for turbines without risking damage. Since this maximum speed does not exceed 26 meters per second, there's no need for data cleaning in this regard.
# MAGIC
# MAGIC However, observing the minimum wind speed value of 0.0 meters per second, and considering that turbines require a minimal amount of power from the grid to operate their electronics, it's understandable why we encounter negative values for lv_activepower_kw. These negative values likely represent the power drawn from the grid to operate the turbine, rather than power fed into the grid, as is commonly observed with negative loads. These occurrences should be taken into account when conducting our predictions.
# MAGIC
# MAGIC Take note of the minimum and maximum values of the power produced (`lv_activepower_kw`). While a negative minimum value like -2.47 kW may indicate that the turbine is supplying power to the grid, the maximum value of 3618.73 kW appears excessively high to simply represent the difference between turbine power consumption and production. It's essential to gain a deeper understanding of our data before proceeding. Let's conduct additional statistical analysis to further explore the dataset.

# COMMAND ----------

# MAGIC %sql
# MAGIC with ws as (SELECT ROUND(wind_speed_ms, 0) AS wind_speed_ms_rounded_value FROM wind_turbine_load_prediction.scada_data_bronze)
# MAGIC select wind_speed_ms_rounded_value, count(*) as num from ws
# MAGIC group by wind_speed_ms_rounded_value
# MAGIC order by wind_speed_ms_rounded_value asc

# COMMAND ----------

# MAGIC %sql
# MAGIC with ws as (SELECT ROUND(lv_activepower_kw, 0) AS lv_activepower_kw_rounded_value FROM wind_turbine_load_prediction.scada_data_bronze)
# MAGIC select lv_activepower_kw_rounded_value, count(*) as num from ws
# MAGIC group by lv_activepower_kw_rounded_value
# MAGIC order by lv_activepower_kw_rounded_value asc

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at the table above, we have 121 times when SCADA data was collected with wind speeds under 1 meters/second. 
# MAGIC
# MAGIC Let's see how many of those values are producing negative loads ( `lv_activepower_kw` values). We need to better understand the distribution of our data before we can use it.
# MAGIC
# MAGIC Remember, negative loads for our data means that the turbine was using using power than it was given, and we dont want these values to be part of our data when creating our prediction model.
# MAGIC
# MAGIC These `lv_activepower_kw` datapoints produce negative load/power values because they are takers and not givers of power.
# MAGIC
# MAGIC We need to remove negative load values from our data. Let's find out at which wind speeds are producing negative load values.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wind_turbine_load_prediction.scada_data_bronze WHERE theoretical_power_curve_kwh < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC We do not have any negative values of `theoretical_power_curve_kwh`, this is good!

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from wind_turbine_load_prediction.scada_data_bronze WHERE lv_activepower_kw < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC Let's find how many negative lv_activepower_kw values we have

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from wind_turbine_load_prediction.scada_data_bronze WHERE lv_activepower_kw < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC There are 57 negative values of `lv_activepower_kw`, let's find out how many zero's we have.
# MAGIC ##### Explore dataset based on `summarize` statistics

# COMMAND ----------

dbutils.data.summarize(df_wind_farm_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC In our summary statistics table, 21% of our `lv_activepower_kw` values are zero. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from wind_turbine_load_prediction.scada_data_bronze WHERE lv_activepower_kw = null;

# COMMAND ----------

# MAGIC %md
# MAGIC We do not have any `null` values of lv_activepower_kw`theoretical_power_curve_kwh`, this is good! However, we do have missing datapoints, we will address this through interpolation later.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display Histogram graph and look for outliers for low voltage active power (lv_activepower_kw)
# MAGIC
# MAGIC The "cut-in speed" denotes the minimum wind speed required for a wind turbine to commence generating electricity efficiently. This parameter is critical in wind turbine design and operation as it dictates the onset of power generation, influencing the overall energy output.
# MAGIC
# MAGIC When wind speeds remain below the cut-in speed, the turbine's blades typically rotate too slowly to generate significant electricity, resulting in either idle operation or very low efficiency.
# MAGIC
# MAGIC Upon examining the data, it appears that turbines continue operating even when the predicted wind speed falls below the turbine's estimated cut-in speed (assumed to be less than 3 meters per second). During these instances, the turbines consume power for their electronics, effectively becoming power consumers rather than producers.
# MAGIC
# MAGIC To ensure data accuracy, we must exclude wind speed values below the turbine's cut-in speed. Unfortunately, the specific cut-in speed for the turbines at this farm is unknown, as the data lacks information regarding the make and model of the turbines, which would provide the manufacturer's cut-in speed rating.
# MAGIC
# MAGIC In lieu of this information, we will employ the commonly used value of 3 meters per second as the cut-in speed. Additionally, we will consider wind speeds exceeding 25 meters per second as potentially damaging to the turbine, prompting shutdown to prevent operational harm.

# COMMAND ----------

import matplotlib.pyplot as plt

# remove values where the wind speed is less than 3 meters per second and above 25 meters per second
df_wind_farm_histogram = spark.sql(
    """
    -- get lv_active_power_kw as a sub query, between a range of window speed
    with lv as (
      SELECT lv_activepower_kw FROM wind_turbine_load_prediction.scada_data_bronze where wind_speed_ms > 2 and wind_speed_ms < 26
    ),
    -- Generate the Historgram Bins
    hist_data as (SELECT
      explode(histogram_numeric(lv_activepower_kw, 40)) as rw
    FROM
      lv
    )
    -- Format the X and Y into an columnar format 
    select hd.rw.x as x, hd.rw.y as y from hist_data hd
    """)

df_wind_farm_histogram.show()

# Use dataframe to plot
pandas_df = df_wind_farm_histogram.toPandas()

# Plot the graph
plt.figure(figsize=(12, 6))
plt.plot(pandas_df["x"], pandas_df["y"], marker='o', linestyle='-')
plt.title("Graph of wind_speed_ms vs lv_activepower_kw")
plt.xlabel("wind_speed_ms")
plt.ylabel("lv_activepower_kw")
plt.grid(True)
plt.show()


# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.sql.functions import col
import seaborn as sns

# Make a new DataFrame to plot the graph
df = df_wind_farm_bronze.withColumn('wind_speed_ms', col('wind_speed_ms'))

# Plot wind speed vs. produced power
plt.figure(figsize=(10, 6))
sns.scatterplot(x='wind_speed_ms', y='lv_activepower_kw', data=df.toPandas())
plt.title('Wind Speed vs. Produced Power')
plt.xlabel('Wind Speed (m/s)')
plt.ylabel('Produced Power (kW)')
plt.grid(True)
plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Notice that produced power (lv_activepower_kw) sometimes produces low power when wind speeds are above 3 meters per second and other times produces high power when wind speeds are less that 3 meters per second. Let's making a graph visulization by plotting all out datapoints.
# MAGIC
# MAGIC The Independent Axis as our wind speed and our Dependent Axis is our produced power (lv_activepower_kw).

# COMMAND ----------



# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Let's look at the interpulated data using SQL
# MAGIC SELECT
# MAGIC     datetime,
# MAGIC     lv_activepower_kw,
# MAGIC     prev_value,
# MAGIC     next_value,
# MAGIC     prev_time,
# MAGIC     next_time,
# MAGIC     prev_value + (next_value - prev_value) * (datetime - prev_time) / (next_time - prev_time) AS interpolated_value
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         datetime,
# MAGIC         lv_activepower_kw,
# MAGIC         LAG(lv_activepower_kw) OVER (ORDER BY datetime) AS prev_value,
# MAGIC         LEAD(lv_activepower_kw) OVER (ORDER BY datetime) AS next_value,
# MAGIC         LAG(datetime) OVER (ORDER BY datetime) AS prev_time,
# MAGIC         LEAD(datetime) OVER (ORDER BY datetime) AS next_time
# MAGIC     FROM
# MAGIC         wind_turbine_load_prediction.scada_data_bronze
# MAGIC ) AS temp
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
