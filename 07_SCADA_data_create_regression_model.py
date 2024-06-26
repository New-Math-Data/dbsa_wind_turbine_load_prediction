# Databricks notebook source
# MAGIC %md
# MAGIC ### Overview - Regression Model
# MAGIC
# MAGIC ##### In this notebook you will:
# MAGIC * Split the cleaned gold data, allocating 75% as our training set and 25% as our test set
# MAGIC * Build a Lienar Regression model to forecast power production using wind speed
# MAGIC * Evaluate how well our Lienar Regression model did by looking at the RMSE (Root Mean Squared Error) and R2 (Coefficient of Determination)
# MAGIC * Build a Random Forest Regression model to forecast power production using wind speed
# MAGIC * Evaluate how well our Random Forest Regression model did by looking at the RMSE (Root Mean Squared Error) and R2 (Coefficient of Determination)

# COMMAND ----------

# Create DataFrame

df_scada_data_gold = spark.sql("""SELECT lv_activepower_kw_hourly_sum, wind_speed_ms_hourly_avg FROM wind_turbine_load_prediction.scada_data_gold""")

# Confirm DataFrame creation
display(df_scada_data_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Split the cleaned gold data, allocating 75% as our training set and 25% as our test set

# COMMAND ----------

# Create training and test DataFrame
df_train, df_test = df_scada_data_gold.randomSplit([.75, .25], seed=42)

print(df_train.cache().count())

# COMMAND ----------

display(df_train.select("lv_activepower_kw_hourly_sum", "wind_speed_ms_hourly_avg").summary())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Build a regression model to forecast power production using wind speed

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import col

# If there are null values in the dataset, the VectorAssembler will encounter an error when trying to assemble the features: handleInvalid=["keep/skip"], `skip` will skip these null values, however, ideally the null values are handled by interpotation in the clean up (Silver) data stage as we did in the Silver layer notebook. 

# Prepare features
feature_columns = ['wind_speed_ms_hourly_avg']
vec_assembler = VectorAssembler(inputCols=feature_columns, outputCol='features', handleInvalid="keep")

df_train_vector = vec_assembler.transform(df_train)

# Create an instance of LinearRegression
lr = LinearRegression(featuresCol='features', labelCol='label')

df_trained_data = df_train_vector.withColumn('label', col('lv_activepower_kw_hourly_sum')).select('features','label')
display(df_trained_data)

# Train the model
lr_model = lr.fit(df_trained_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Evaluate how well our model did by looking at the RMSE (Root Mean Squared Error)
# MAGIC
# MAGIC The RMSE measures the average magnitude of the errors (residuals) between the actual values and the predicted values produced by the regression model. 
# MAGIC
# MAGIC We are looking for a low RMSE value, the lower the RMSE the better our regression model preformed.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import avg, col

df_test_vector = vec_assembler.transform(df_test)
df_test_vec = df_test_vector.withColumn('label', col('lv_activepower_kw_hourly_sum')).select('features','label')

display(df_test_vec)

df_pred = lr_model.transform(df_test_vec)
display(df_pred)

# Calculate the average of the "label" column in the DataFrame with predictions
df_average_label_pred = df_pred.agg(avg(col("label"))).collect()[0][0]
print("Average label with predictions:", df_average_label_pred)

regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")

# Ideally RMSE is under the average of the predicted column (active power)
rmse = regression_evaluator.evaluate(df_pred)
# Ideally R^2 should be close to one, indicating a better fit of the model to the data
r2 = regression_evaluator.setMetricName("r2").evaluate(df_pred)

print(f"RMSE is {rmse}")
print(f"R2 is {r2}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Inspect the trained model

# COMMAND ----------

for col, coef in zip(vec_assembler.getInputCols(), lr_model.coefficients):
    print(col, coef)

# Get the slope (m) and intercept (b)
m = lr_model.coefficients[0]
b = lr_model.intercept

print(f"linear regression line is y = {m:.2f}x + {b:.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Build a Random Forest Regression

# COMMAND ----------

from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import avg, col

# Prepare features
feature_columns = ['wind_speed_ms_hourly_avg']
vec_assembler = VectorAssembler(inputCols=feature_columns, outputCol='features', handleInvalid="keep")

df_train_vector = vec_assembler.transform(df_train)

# Create an instance of Random Forest Regression
rf = RandomForestRegressor(featuresCol="features", labelCol="label", numTrees=100)

df_trained_data = df_train_vector.withColumn('label', col('lv_activepower_kw_hourly_sum')).select('features','label')

display(df_trained_data)

# Train the model
rf_model = rf.fit(df_trained_data)

df_test_vector = vec_assembler.transform(df_test)

df_test_vec = df_test_vector.withColumn('label', col('lv_activepower_kw_hourly_sum')).select('features','label')

display(df_test_vec)

df_pred = rf_model.transform(df_test_vec)
display(df_pred)

# Calculate the average of the "label" column in the DataFrame with predictions
df_average_label_pred = df_pred.agg(avg(col("label"))).collect()[0][0]
print("Average label with predictions:", df_average_label_pred)

regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="label", metricName="rmse")

# Ideally RMSE is under the average of the predicted column (active power)
rmse = regression_evaluator.evaluate(df_pred)
# Ideally R^2 should be close to one, indicating a better fit of the model to the data
r2 = regression_evaluator.setMetricName("r2").evaluate(df_pred)

print(f"Random Forest Regression RMSE is {rmse}")
print(f"Random Forest Regression R^2 is {r2}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Random Forest Regression model has a higher R^2, we will use this model over the Linear Regression model
