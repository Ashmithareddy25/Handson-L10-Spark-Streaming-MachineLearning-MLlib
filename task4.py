# ===========================================================
# ITCS 6190/8190 - Spark Structured Streaming with MLlib
# Task 4: Real-Time Fare Prediction
# ===========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, abs as abs_diff, from_json
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel

# -----------------------------------------------------------
# STEP 1: Initialize Spark session
# -----------------------------------------------------------
spark = SparkSession.builder \
    .appName("Task4_RealTime_Fare_Prediction") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# -----------------------------------------------------------
# STEP 2: Train and save model using static CSV
# -----------------------------------------------------------
print("=== Training model using training-dataset.csv ===")

train_df = spark.read.csv("training-dataset.csv", header=True, inferSchema=True)
assembler = VectorAssembler(inputCols=["distance_km"], outputCol="features")
train_vec = assembler.transform(train_df)

lr = LinearRegression(featuresCol="features", labelCol="fare_amount")
model = lr.fit(train_vec)

# Save trained model
model.write().overwrite().save("models/fare_model")
print("✅ Model trained and saved to models/fare_model")

# -----------------------------------------------------------
# STEP 3: Load model for real-time prediction
# -----------------------------------------------------------
loaded_model = LinearRegressionModel.load("models/fare_model")

# Define schema of incoming stream
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True)
])

# -----------------------------------------------------------
# STEP 4: Read streaming data from socket
# -----------------------------------------------------------
print("=== Waiting for streaming data from localhost:9999 ===")
stream_df = spark.readStream.format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

# Parse JSON lines coming from data_generator.py
parsed_df = stream_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# -----------------------------------------------------------
# STEP 5: Apply model to predict fare
# -----------------------------------------------------------
assembled_stream = VectorAssembler(inputCols=["distance_km"], outputCol="features").transform(parsed_df)
pred_df = loaded_model.transform(assembled_stream)

# Compute deviation (difference between actual and predicted fare)
result_df = pred_df.withColumn("deviation", abs_diff(col("fare_amount") - col("prediction")))

# -----------------------------------------------------------
# STEP 6: Display output on console
# -----------------------------------------------------------
query = result_df.select("ride_id", "distance_km", "fare_amount", "prediction", "deviation") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
