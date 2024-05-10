# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Data cleaning, transformation, wrangling and normalizing
# MAGIC
# MAGIC Assumputions are made that the dataset used has turbines of all the same make, model and generator capacity.
# MAGIC
# MAGIC ##### In this notebook we will:
# MAGIC   * Remove invalid charactors in header
# MAGIC   * Look for null or zero values in the data that will cause outlines or skew the data
# MAGIC   * Explore dataset based on summary statistics
# MAGIC   * Identify and remove outliers in a dataset
# MAGIC   * Impute missing data 
# MAGIC   * Create an imputer pipeline using Spark ML   

# COMMAND ----------

# MAGIC %md
# MAGIC **Remove invalid charactors in header**
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
