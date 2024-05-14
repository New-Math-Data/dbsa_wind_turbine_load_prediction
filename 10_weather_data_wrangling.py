# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - 
# MAGIC
# MAGIC When we normalize data, we are adjusting the scale of the data to a common scale without distorting differences in the ranges of values. This process is known as "normalization."
# MAGIC
# MAGIC Normalization ensures that all features contribute equally to the analysis and prevents features with larger scales from dominating those with smaller scales. It's a common practice in various machine learning algorithms to ensure that the data is within a consistent range.
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * min-max scaling, 
# MAGIC * z-score normalization (standardization),
# MAGIC * feature scaling by dividing by the maximum absolute value
# MAGIC * feature store? 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transform Data Types to correct values
# MAGIC
# MAGIC Take a look at the table values. You'll notice that the **`Date/Time`** column was assigned a `string` data type. We only need the last two digits of this field, For our task, we will need to group by hour rather than every 10 minutes. This is becuase our forcasted with data is forcasted by the hour. 
# MAGIC
# MAGIC Let's transform **Date/Time** by grouping by hours.

# COMMAND ----------

from pyspark.sql.functions import col, translate

transform_df = base_df.withColumn("Date/Time", translate(col("Date/Time"), "[INSERT]", "").cast("double"))

display(transform_df)

# COMMAND ----------



# COMMAND ----------


