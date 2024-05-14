# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Regression: Predict Wind Turbine Farm power produced 
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * Create a linear regression model using SparkML

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create a linear regression model using SparkML

# COMMAND ----------

# Create DataFrame
df_scada_data_gold = spark.sql("""SELECT datetime, wind_speed_ms, lv_activepower_kw, wind_direction FROM wind_turbine_load_prediction.scada_data_gold""")

# Show the DataFrame
display(df_scada_data_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's spilt our cleaned data using 80% of it was our training set, and 20% as our test set

# COMMAND ----------

train_df, test_df = df_scada_data_gold.randomSplit([.75, .25], seed=42)
print(train_df.cache().count())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's build our model predicting power produced given wind speed

# COMMAND ----------

display(train_df.select("lv_activepower_kw", "wind_speed_ms"))

# COMMAND ----------

display(train_df.select("lv_activepower_kw", "wind_speed_ms").summary())

# COMMAND ----------

# MAGIC %md
# MAGIC Final datapoint left after cleaning and applying conventional standards to the data: 29,811 datapoints.

# COMMAND ----------

display(train_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### The Linear Regression estimator (.fit()) expects a column of Vector type as input. Let's update this now.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler

vec_assembler = VectorAssembler(inputCols=["wind_speed_ms", "lv_activepower_kw"], outputCol="features")

vec_train_df = vec_assembler.transform(train_df)

# COMMAND ----------

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(featuresCol="features", labelCol="lv_activepower_kw")

lr_model = lr.fit(train_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's inspect our Model and apply the model to the test set

# COMMAND ----------

m = lr_model.coefficients[0]
b = lr_model.intercept

print(f"linear regression line is y = {m:.2f}x + {b:.2f}")

# COMMAND ----------

vec_test_df = vec_assembler.transform(test_df)

pred_df = lr_model.transform(vec_test_df)

pred_df.select("wind_speed_ms", "features", "lv_activepower_kw", "prediction").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Let's evaluate how well our model did by looking at the RMSE (Root Mean Squared Error).
# MAGIC
# MAGIC The RMSE measures the average magnitude of the errors (residuals) between the actual values and the predicted values produced by the regression model. 
# MAGIC
# MAGIC We are looking for a low RMSE value, the lower the RMSE the better our regression model preformed.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator

regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="lv_activepower_kw", metricName="rmse")

rmse = regression_evaluator.evaluate(pred_df)
print(f"RMSE is {rmse}")
