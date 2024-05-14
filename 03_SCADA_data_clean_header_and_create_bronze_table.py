# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Clean header and create Bronze Delta Table
# MAGIC Removing invalid characters from the DataFrame.
# MAGIC
# MAGIC ##### In this notebook we will:
# MAGIC   * Remove invalid charactors from header
# MAGIC   * Explore Statistical moments by running Databricks built-in commands `describe` and `summary`
# MAGIC   * Create Bronze Delta Table

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's first run the SCADA_data_ingest notebook to get the dataframe

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/02_SCADA_data_ingest"

# COMMAND ----------

# MAGIC %md
# MAGIC **Remove invalid charactors from header**

# COMMAND ----------

# Show the current header
display(df_wind_farm_raw.head())

# COMMAND ----------

import re
from functools import reduce

# Define a function to clean column names
def clean_header(header):
    # Replace all other whitespace characters with underscores
         # Remove trailing whitespace
    clean_header = re.sub(r'[^\w\s]', '', header.replace(' ', '_').lower())
    cleaned_header = re.sub(r'_$', '', clean_header)
    return cleaned_header

# Clean column names
renamed_columns = [(col, clean_header(col)) for col in df_wind_farm_raw.columns]
df_cleaned_header = reduce(lambda data, column: data.withColumnRenamed(*column), renamed_columns, df_wind_farm_raw)

# Show the DataFrame with cleaned column names
display(df_cleaned_header.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Determine Statistic measures of the datasets by running Databricks built-in commands 'Describe' and 'Summary'
# MAGIC
# MAGIC Two options:
# MAGIC * **`describe`**: count, mean, stddev, min, max
# MAGIC * **`summary`**: describe + interquartile range (IQR)

# COMMAND ----------

display(df_cleaned_header.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the `min` and `max` values of `lv_activepower_kw`. The negative -2.47 kW minimum value can make sence if a negative value represents when the turbine is a `giver` of load to the grid, but now look at the maxium value of 3618.73 kW, this value is too large to be the value that represents the difference between what is takes to power the turbine verses the power it produces. We need to better understand our data before we use it, lets run some more statistics.  

# COMMAND ----------

display(df_cleaned_header.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC When the standard deviation and the mean are close in value, it typically indicates that the data points are tightly clustered around the mean. This implies that the distribution of the data is relatively narrow and that there is less variability or dispersion among the values. This making for good data distribution.
# MAGIC
# MAGIC Notice that the max wind speed meters/second is `25.21`, knowing that this value is also the max value that turbines can safetly run at before they obtain damage, and that the max value is does not exceed 26 meters/second, there's not need to clean any data here. 
# MAGIC
# MAGIC However, looking at the `min` value at `0.0` for `wind speed`, and knowing that turbines require a small amount of power from the grid to power their electronics, it makes sence why we are seeing negative values for `lv_activepower_kw`. It appears the `negative` values represent the power taken from the grid to power the turbine, and not the power given to the grid, as we usually see when loads are nagative. These values will need to be taken into consideration when we do our predictions.

# COMMAND ----------

# MAGIC %md
# MAGIC ####**Create Bronze Delta Table**
# MAGIC Two steps:
# MAGIC * Create a Schema/Database for the table to reside in permanently
# MAGIC * Create and Load raw data into a Delta table for analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a Schema/database for the tables to reside in permanently

# COMMAND ----------

# MAGIC %sql 
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop a database and all its objects
# MAGIC -- DROP DATABASE my_database RESTRICT;
# MAGIC -- DROP DATABASE my_database CASCADE;

# COMMAND ----------

# Create a Schema (aka Database) for the Table

# Create Schema if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS wind_turbine_load_prediction LOCATION '/main'")

# USE command makes sure that future code blocks are being run under the correct Schema
spark.sql(f"USE wind_turbine_load_prediction")

# COMMAND ----------

# MAGIC %md
# MAGIC **Create and Load data into a Delta table for analysis**

# COMMAND ----------

# Create a permanent delta table (Bronze table (raw data) by converting the Spark DataFrame we created eariler to a Delta Table

# Override any pervious created table and register the DataFrame as a Delta table in the metastore 
df_cleaned_header.write.format("delta").mode("overwrite").saveAsTable("scada_data_bronze")

print("Table 'scada_data_bronze'exists.")

# Lets remove the DataFrame from session storage
dbutils.fs.rm(dir=file_path, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Query the table to verify creation and validitity**

# COMMAND ----------

# Verify the table has been created by querying the table
spark.sql("SELECT * FROM scada_data_bronze LIMIT 10").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
