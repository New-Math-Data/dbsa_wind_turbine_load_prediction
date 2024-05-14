# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Data wrangling and create Silver Table
# MAGIC Data Wrangling is a term used to describe the process of cleaning, transforming, and preparing raw data into a format suitable for analysis. Data wrangling involves various tasks such as handling missing values, restructuring data, merging datasets, and converting data types. It's a crucial step in the data analysis pipeline, ensuring that the data is accurate, complete, and well-organized before performing further analysis or modeling.
# MAGIC
# MAGIC Assumputions are made that the dataset used has turbines of all the same make, model and generator capacity.
# MAGIC
# MAGIC ##### In this notebook we will:
# MAGIC   * Look for null or zero values in the data that will cause outlines or skew the data
# MAGIC   * Explore dataset based on `summarize` statistics
# MAGIC   * Identify and remove outliers in a dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Explore the dataset and relationships for any needed future cleaning/wrangling/normalizing by making a temperary DataFrame that we can clean before writting it to a Silver Table

# COMMAND ----------

# Check to make sure the data looks correct and look for any data cleaning and normalization we need to do later

# Lets make a temperary DataFrame that we can use to clean our data
# Create DataFrame
df_wind_farm_bronze = spark.sql("""SELECT * FROM wind_turbine_load_prediction.scada_data_bronze""")

# Show the DataFrame
display(df_wind_farm_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Remove Data Outliers
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

# MAGIC %sql 
# MAGIC
# MAGIC WITH boxplot_stats AS (
# MAGIC   SELECT
# MAGIC     MIN( wind_turbine_load_prediction.scada_data_bronze.lv_activepower_kw) AS min_value,
# MAGIC     MAX( wind_turbine_load_prediction.scada_data_bronze.lv_activepower_kw) AS max_value,
# MAGIC     PERCENTILE( wind_turbine_load_prediction.scada_data_bronze.lv_activepower_kw, 0.25) AS q1,
# MAGIC     PERCENTILE( wind_turbine_load_prediction.scada_data_bronze.lv_activepower_kw, 0.5) AS median,
# MAGIC     PERCENTILE( wind_turbine_load_prediction.scada_data_bronze.lv_activepower_kw, 0.75) AS q3
# MAGIC   FROM
# MAGIC      wind_turbine_load_prediction.scada_data_bronze
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM boxplot_stats;
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import min as spark_min, max as spark_max, expr

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BoxplotStats") \
    .getOrCreate()

# Calculate boxplot statistics
boxplot_stats = spark.table("wind_turbine_load_prediction.scada_data_bronze") \
    .select(
        spark_min("lv_activepower_kw").alias("min_value"),
        spark_max("lv_activepower_kw").alias("max_value"),
        expr("percentile_approx(lv_activepower_kw, 0.25)").alias("q1"),
        expr("percentile_approx(lv_activepower_kw, 0.5)").alias("median"),
        expr("percentile_approx(lv_activepower_kw, 0.75)").alias("q3")
    )

# Show the result
boxplot_stats.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display a Histogram table and look for outliers for low voltage active power (lv_activepower_kw)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC     with lv as (
# MAGIC       SELECT lv_activepower_kw FROM wind_turbine_load_prediction.scada_data_bronze
# MAGIC     ),
# MAGIC     hist_data as
# MAGIC      (SELECT
# MAGIC       explode(histogram_numeric(lv_activepower_kw, 20)) as rw
# MAGIC     FROM
# MAGIC       lv
# MAGIC     )
# MAGIC     select hd.rw.x as x, hd.rw.y as y from hist_data hd
# MAGIC
# MAGIC   

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, histogram_numeric

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Histogram") \
    .getOrCreate()

# Load the scada_data_bronze DataFrame and select lv_activepower_kw column
lv = spark.table("wind_turbine_load_prediction.scada_data_bronze").select("lv_activepower_kw")

# Calculate histogram using histogram_numeric function and explode the result
hist_data = lv.select(explode(histogram_numeric(col("lv_activepower_kw"), 20)).alias("rw"))

# Select x and y values from the exploded DataFrame
result = hist_data.select(col("rw.x").alias("x"), col("rw.y").alias("y"))

# Show the result
result.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Display Histogram graph and look for outliers for low voltage active power (lv_activepower_kw)
# MAGIC
# MAGIC The "cut-in speed" of a wind turbine refers to the minimum wind speed at which the turbine starts to generate electricity efficiently. 
# MAGIC
# MAGIC The cut-in speed is an important parameter in wind turbine design and operation because it determines when the turbine begins to produce power and contribute to the overall energy output. 
# MAGIC
# MAGIC Below the cut-in speed, the turbine's blades typically do not rotate fast enough to generate sufficient electricity, so the turbine remains idle or operates at very low efficiency.
# MAGIC
# MAGIC We need to remove the wind speed values below the turbine cut-in speed, unfortunately this value is not known as the data does not provide us with the Make and Model of the farms turbines, hence we do not have the manufactures rating for cut-in speed.
# MAGIC
# MAGIC Instead we will explore the data futher to determine an appropriate cut-in speed.
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt

df = spark.sql(
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

df.show()

# Use dataframe to plot
pandas_df = df.toPandas()

# Plot the graph
plt.figure(figsize=(12, 6))
plt.plot(pandas_df["x"], pandas_df["y"], marker='o', linestyle='-')
plt.title("Graph of wind_speed_ms vs lv_activepower_kw")
plt.xlabel("wind_speed_ms")
plt.ylabel("lv_activepower_kw")
plt.grid(True)
plt.show()


# COMMAND ----------

# MAGIC %sql
# MAGIC with ws as (SELECT ROUND(wind_speed_ms, 0) AS wind_speed_ms_rounded_value FROM wind_turbine_load_prediction.scada_data_bronze)
# MAGIC select wind_speed_ms_rounded_value, count(*) as num from ws
# MAGIC group by wind_speed_ms_rounded_value
# MAGIC order by wind_speed_ms_rounded_value asc

# COMMAND ----------

# MAGIC %md
# MAGIC Looking at the table above, we have 121 times when SCADA data was collected with wind speeds under 1 meters/second. 
# MAGIC
# MAGIC Let's see how many of those values are producing negative loads ( `lv_activepower_kw` values). We need to better understand the distribution of our data before we can use it.
# MAGIC
# MAGIC Remember, negative loads for our data means that the turbine was using using power than it was given, and we dont want these values to be part of our data when creating our prediction model.
# MAGIC
# MAGIC Unfortunately, the turbines do not turn off when the predicted wind speed is set to be less than the turbine's cut-in speed ( estimated to be less than 3 meters/second). The turbines will consume power to power their electronics, therefore they become consumers of power and not producers during these times when their cut-in speed is not reached.
# MAGIC
# MAGIC These `lv_activepower_kw` datapoints produce negative load/power values because they are takers and not givers of power.
# MAGIC
# MAGIC We need to remove negative load values from our data. 
# MAGIC
# MAGIC Let's find out at which wind speeds are producing negative load values.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from wind_turbine_load_prediction.scada_data_bronze WHERE lv_activepower_kw < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's find how many negative values we have

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) from wind_turbine_load_prediction.scada_data_bronze WHERE lv_activepower_kw < 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### There are 57 negative values of `lv_activepower_kw`, let's find out how many zero's we have.
# MAGIC ##### Explore dataset based on `summarize` statistics

# COMMAND ----------

dbutils.data.summarize(df_wind_farm_bronze)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from wind_turbine_load_prediction.scada_data_bronze WHERE lv_activepower_kw = null;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### In our summary statistics table, 21% of our `lv_activepower_kw` values are zero. 
# MAGIC
# MAGIC In perpreation of creating our Silver table we will:
# MAGIC
# MAGIC * Remove the `lv_activepower_kw` values that are equal to and less than zero, this will include the negative values. 
# MAGIC * We will insert the interpolated values for `lv_activepower_kw` missing values. 

# COMMAND ----------

from pyspark.sql.functions import col

df_wind_farm_bronze = df_wind_farm_bronze.filter(col('lv_activepower_kw') > 0)

# Show the DataFrame
display(df_wind_farm_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that all the negative and zero values are removed from our dataset, notice that produced power (`lv_activepower_kw`) sometimes produces low power when wind speeds are above 3 meters per second and other times produces high power when wind speeds are less that 3 meters per second. Let's making a graph visulization by plotting all out datapoints.
# MAGIC
# MAGIC The Independent Axis as our wind speed and our Dependent Axis is our produced power (`lv_activepower_kw`).

# COMMAND ----------

import matplotlib.pyplot as plt
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
# MAGIC ##### Let's look at using the linear interpolation approach to interpolate missing rows
# MAGIC
# MAGIC There are missing datapoints in the dataset when an incemental 10 min reading is missing. 
# MAGIC
# MAGIC These missing data points may be due to a varety of reasons, including, low wind speed compared to the turbine cut-in speed, malfunction of the turbine, system operational error, sensor malfunction or wind turbine maintenance. 
# MAGIC
# MAGIC Even though the mean and standard deviation are close in values, we shouldn't be skipping values when building our model. Missing values could raise the uncertainty of the prediction model, lets use the dataset's median to replace these outliers. 
# MAGIC
# MAGIC We can do this by using the linear interpolation approach to supplement missing values. Linear interpolation is based on data before and after the missing values, as well as theoretical information. Here "theoretical information" is the slope of the data to the nearest non-missing value.

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

df_wind_farm_bronze.show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer
from pyspark.sql.window import Window

# Convert datetime string to timestamp type
df_wind_farm_bronze = df_wind_farm_bronze.withColumn("datetime", to_timestamp("datetime", format="dd MM yyyy HH:mm"))

# Convert timestamp string to unix_timestamp type
df_wind_farm_bronze_epoch = df_wind_farm_bronze.withColumn("datetime_epoch", unix_timestamp("datetime"))

display(df_wind_farm_bronze_epoch)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Before we preform inturpulation of our data, let's look to see if we need all of our columns for our model, are there any columns we can drop? 
# MAGIC
# MAGIC Later we want to compare the performance of our linear regression model against the theoretical maximum power output predicted by the theoretical power curve, we are going to keep the `theoretical_power_curve_kwh` column, becuase theoretical maximum power is dependent on the `wind_direction` we need to keep this column as well.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer
from pyspark.sql.window import Window

# Let's look at the interpulated data using Spark

# Ensure DataFrame is sorted by time
df_wind_farm_bronze = df_wind_farm_bronze.orderBy("datetime_epoch")

#df_wind_farm_bronze.show()

# Fill in missing timestamps(datetimes) 
# Here we want to fill in missing values for every 10 minutes (600 seconds)
min_timestamp = df_wind_farm_bronze.selectExpr("min(datetime_epoch) as min_datatime_epoch").first()[0]
max_timestamp = df_wind_farm_bronze.selectExpr("max(datetime_epoch) as max_datatime_epoch").first()[0]

all_timestamps = spark.range(min_timestamp, max_timestamp, step=600)

all_timestamps.show()

# Left join with original DataFrame to ensure all timestamps are present
df_wind_farm_bronze = all_timestamps.join(df_wind_farm_bronze,df_wind_farm_bronze["datetime_epoch"] == all_timestamps["id"], "left")



# # Perform interpolation using linear interpolation
# window = Window.orderBy("datetime_epoch").rowsBetween(-1, 1)

# df_wind_farm_bronze = df_wind_farm_bronze.withColumn("interpolated_value", coalesce(df_wind_farm_bronze["wind_speed_ms", "lv_activepower_kw"], (lag(df_wind_farm_bronze["wind_speed_ms", "lv_activepower_kw"]).over(window) + lead(df_wind_farm_bronze["wind_speed_ms", "lv_activepower_kw"]).over(window)) / 2))

# Show the result
display(df_wind_farm_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Databrick's ML API Transformer and Estimator can do the linear interpolation for us
# MAGIC
# MAGIC Transformer: Transforms one DataFrame into another DataFrame. It accepts a DataFrame as input, and returns a new DataFrame with one or more columns appended to it. Transformers do not learn any parameters from your data and simply apply rule-based transformations. It has a .transform() method.
# MAGIC
# MAGIC Estimator: An algorithm which can be fit on a DataFrame to produce a Transformer. E.g., a learning algorithm is an Estimator which trains on a DataFrame and produces a model. It has a .fit() method because it learns (or "fits") parameters from your DataFrame.
# MAGIC
# MAGIC Let's use Databrick's ML API Estimator algorithm along with Transformer, to transform our DataFrame creating a good fit model for our data. 
# MAGIC

# COMMAND ----------

from pyspark.ml.feature import Imputer
from pyspark.sql.window import Window

# Let's look at the interpulated data using Databrick's Transformer and Estimator functions

# Convert timestamp string to timestamp type
df = df.withColumn("timestamp", to_timestamp("timestamp"))


imputer = Imputer(strategy="median", inputCols=['lv_activepower_kw', 'wind_speed_ms'], outputCols=['lv_activepower_kw', 'wind_speed_ms'])

imputer_model = imputer.fit(df_wind_farm_bronze).transform(df_wind_farm_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Great, our data is cleansed now! Let's save this DataFrame to a Silver Delta Table so that we can start building models with it.

# COMMAND ----------

# Create a permanent delta table (Silver table (cleaned data) by converting the Spark DataFrame we created eariler to a Delta Table

spark.sql(f"USE wind_turbine_load_prediction")
df_cleaned.write.format("delta").mode("overwrite").saveAsTable("scada_data_silver")

# There's is no need to check if the table exists becuause either way we would like to overide it as we may have made additional filters 

# Override the table and register the DataFrame as a Delta table in the metastore
df_cleaned.write.format("delta").mode("overwrite").saveAsTable("scada_data_silver")

print("Table 'scada_data_silver' exists.")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM wind_turbine_load_prediction.scada_data_silver;

# COMMAND ----------

# MAGIC %md
# MAGIC
