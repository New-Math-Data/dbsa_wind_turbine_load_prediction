# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Data wrangling and create Silver Table
# MAGIC Data Wrangling is a term used to describe the process of cleaning, transforming, and preparing raw data into a format suitable for analysis. Data wrangling involves various tasks such as handling missing values, restructuring data, merging datasets, and converting data types. It's a crucial step in the data analysis pipeline, ensuring that the data is accurate, complete, and well-organized before performing further analysis or modeling.
# MAGIC
# MAGIC In perpreation of creating our Silver table we will:
# MAGIC
# MAGIC * When the speed of wind is below the wind turbine's cut-in-speed, the turbine should produce zero active power, we will replace the wind turbine output power (`lv_activepower_kw`) values with Zero.
# MAGIC * When the active power is more than the wind turbine's installed capacity, the rated installed capacity value is utilized instead.
# MAGIC * We will replace the `lv_activepower_kw` negative values with Zero. 
# MAGIC * We will use linear interpolation to insert the interpolated values for `lv_activepower_kw`'s missing values. 

# COMMAND ----------

# Create a temporary DataFrame for data cleaning purposes
df_wind_farm_bronze = spark.sql("""SELECT * FROM wind_turbine_load_prediction.scada_data_bronze""")

# Show the DataFrame
display(df_wind_farm_bronze)

# COMMAND ----------

from pyspark.sql.functions import col, when

# Replace negative values of lv_activepower_kw with zero
df_replaced_neg_values = df_wind_farm_bronze.withColumn("lv_activepower_kw", when(df_wind_farm_bronze["lv_activepower_kw"] < 0, 0).otherwise(df_wind_farm_bronze["lv_activepower_kw"]))

# Show the DataFrame
display(df_replaced_neg_values)

# COMMAND ----------

dbutils.data.summarize(df_replaced_neg_values)

# COMMAND ----------

from pyspark.sql.functions import col, when

# When the active power is more than the wind turbine's installed capacity, the rated installed capacity value is utilized instead

# Observing the maximum wind speed, it's evident that our data does not reach the 4MW maximum rated capacity
df_replaced_above_capacity = df_replaced_neg_values.withColumn("lv_activepower_kw", when(df_replaced_neg_values["lv_activepower_kw"] > 4000, 4000).otherwise(df_replaced_neg_values["lv_activepower_kw"])) 


# Show the DataFrame
display(df_wind_farm_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that all the negative values are removed from our dataset

# COMMAND ----------

dbutils.data.summarize(df_replaced_above_capacity)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer
from pyspark.sql.window import Window

# Convert datetime string to timestamp type
df_convert_datatime_to_timestamp = df_replaced_above_capacity.withColumn("datetime", to_timestamp("datetime", format="dd MM yyyy HH:mm"))

# Convert timestamp string to unix_timestamp type
df_convert_timestamp_to_epoch = df_convert_datatime_to_timestamp.withColumn("datetime_epoch", unix_timestamp("datetime"))

display(df_convert_timestamp_to_epoch)

# COMMAND ----------

# MAGIC %md
# MAGIC Before we proceed with data interpolation, let's assess if there are any columns we can discard for our model. We will retain the 'theoretical_power_curve_kwh' column as we intend to compare our linear regression model's performance against the theoretical maximum power output predicted by the power curve. Additionally, since the theoretical maximum power is contingent on wind direction, we will retain the 'wind_direction' column.

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

from pyspark.sql.functions import *
from pyspark.ml.feature import Imputer
from pyspark.sql.window import Window

# Fill in missing timestamps(datetimes) 

# Ensure DataFrame is sorted by time
df_timestamp_sort = df_convert_timestamp_to_epoch.orderBy("datetime_epoch")

#df_wind_farm_cleanup_epoch.show()

# Here we want to fill in missing values for every 10 minutes (600 seconds)
min_timestamp = df_timestamp_sort.selectExpr("min(datetime_epoch) as min_datatime_epoch").first()[0]
max_timestamp = df_timestamp_sort.selectExpr("max(datetime_epoch) as max_datatime_epoch").first()[0]

all_timestamps = spark.range(min_timestamp, max_timestamp, step=600).select(col("id").alias("timestamp"))

display(all_timestamps)

# Left join with original DataFrame to ensure all timestamps are present
df_all_timestamps = all_timestamps.join(df_timestamp_sort, df_timestamp_sort["datetime_epoch"] == all_timestamps["timestamp"], "left")

# Show the result
display(df_all_timestamps)

# COMMAND ----------


# Let's drop `datetime_epoch`
df_datetime_epoch_drop = df_all_timestamps.drop("datetime_epoch")

# Calculate the missing datetimes using the new epoch timestamp column
df_added_missing_datetimes = df_datetime_epoch_drop.withColumn("datetime",when(df_datetime_epoch_drop['datetime'].isNull(), from_unixtime(df_datetime_epoch_drop['timestamp'])).otherwise(df_datetime_epoch_drop['datetime']))


display(df_added_missing_datetimes)

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

# Let's look at the interpulated data using Databrick's Transformer and Estimator functions

# # Create an Imputer instance
imputer = Imputer(strategy="median", inputCols=['lv_activepower_kw', 'wind_speed_ms', 'wind_direction', 'theoretical_power_curve_kwh'], outputCols=['lv_activepower_kw', 'wind_speed_ms', 'wind_direction', 'theoretical_power_curve_kwh'])

# Fit the Imputer to the data
imputer_model = imputer.fit(df_added_missing_datetimes)

# Transform the data to impute missing values
df_imputed = imputer_model.transform(df_added_missing_datetimes)

display(df_imputed)

# COMMAND ----------

# MAGIC %md
# MAGIC Excellent, our data is now cleaned! Let's preserve this DataFrame into a Silver Delta Table, enabling us to commence model building. After cleaning, we now have 52559 datapoints.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS wind_turbine_load_prediction.scada_data_silver;

# COMMAND ----------

# Create a permanent delta table (Silver table (cleaned data) by converting the Spark DataFrame we created eariler to a Delta Table

spark.sql(f"USE wind_turbine_load_prediction")

# There's is no need to check if the table exists becuause either way we would like to overide it as we may have made additional filters 

# Override the table and register the DataFrame as a Delta table in the metastore
df_imputed.write.format("delta").mode("overwrite").saveAsTable("scada_data_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the table was created
# MAGIC SELECT * FROM wind_turbine_load_prediction.scada_data_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify we have more data points than when we started
# MAGIC SELECT count(*) FROM wind_turbine_load_prediction.scada_data_silver;
