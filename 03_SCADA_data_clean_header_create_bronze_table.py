# Databricks notebook source
# MAGIC %md
# MAGIC You may find this series of notebooks at https://github.com/New-Math-Data/dbsa_wind_turbine_load_prediction.git

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overview - Remove Invalid Characters from the header and Create Bronze Delta Table
# MAGIC
# MAGIC ##### In this notebook, we will:
# MAGIC   * Clean the DataFrame by removing invalid characters from the header
# MAGIC   * Examine statistical metrics utilizing Databricks' integrated commands `describe` and `summary` for potential future data manipulation.
# MAGIC     * `describe`: Provides statistics including count, mean, standard deviation, minimum, and maximum.
# MAGIC     * `summary`: describe + interquartile range (IQR)
# MAGIC   * Create the Bronze Delta Table
# MAGIC     * Create a Schema/Database for the table's permanent residence
# MAGIC     * Formulate and Integrate raw data into a Delta [Bronze] table for analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ##### First, run the SCADA_data_ingest notebook to get the perviously created Delta DataFrame

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/rnieder@newmathdata.com/dbsa_wind_turbine_load_prediction/02_SCADA_data_ingest"

# COMMAND ----------

# Show the current header
display(df_wind_farm_raw.head())

# COMMAND ----------

import re
from functools import reduce

# Clean the DataFrame by removing invalid characters from the header

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

# The Databricks `describe` function provides statistics about the dataset, including count, mean, standard deviation, minimum, and maximum
display(df_cleaned_header.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC When the standard deviation and the mean closely align, it typically suggests that the data points are tightly clustered around the mean. This indicates a relatively narrow distribution of data with reduced variability or dispersion among values, which is indicative of a well-behaved data distribution.

# COMMAND ----------

# The Databricks summary function extends the describe functionality by including the interquartile range (IQR)
display(df_cleaned_header.summary())

# COMMAND ----------

# MAGIC %md
# MAGIC Observe the significant variance between Q3 and Q1, implying a wide dispersion or variability within the dataset. We are comfortable with this distribution, as we understand that the optimal power production occurs at wind speeds exceeding 5 meters per second.

# COMMAND ----------


# Create a Schema/Database for the table's permanent residence if not already exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS wind_turbine_load_prediction LOCATION '/main'")

# Confirm Schema was created
spark.sql(f"SHOW DATABASES")

# The USE command ensures that subsequent code blocks are executed within the appropriate Schema.
spark.sql(f"USE wind_turbine_load_prediction")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS wind_turbine_load_prediction.scada_data_bronze;

# COMMAND ----------

# Structure and integrate raw data into a Delta [Bronze] table for analysis

# Establish a persistent delta table (Bronze table - raw data) by converting the previously created Spark DataFrame into a Delta Table.

# Replace any previously existing table and register the DataFrame as a Delta table in the metastore.
df_cleaned_header.write.format("delta").mode("overwrite").saveAsTable("scada_data_bronze")

print("Table 'scada_data_bronze'exists.")

# Clean up: Remove the DataFrame from session storage
dbutils.fs.rm(dir=file_path, recurse=True)

# COMMAND ----------

# Verify the table has been created by querying the table
spark.sql("SELECT * FROM scada_data_bronze LIMIT 10").show()

# COMMAND ----------

# MAGIC %md
# MAGIC
