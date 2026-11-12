import sys
sys.setrecursionlimit(10000)

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from pyspark.ml.pipeline import PipelineModel

print("Starting Spark Streaming Session (Version 3.5.0)...")
spark = SparkSession.builder \
    .appName("RealTimeFraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

print("Loading the trained Random Forest model...")
model = PipelineModel.load("best_fraud_model")

schema = StructType() \
    .add("step", IntegerType()) \
    .add("type", StringType()) \
    .add("amount", DoubleType()) \
    .add("nameOrig", StringType()) \
    .add("oldbalanceOrg", DoubleType()) \
    .add("newbalanceOrig", DoubleType()) \
    .add("nameDest", StringType()) \
    .add("oldbalanceDest", DoubleType()) \
    .add("newbalanceDest", DoubleType())

print("Connecting to Kafka live stream...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions_input") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Make Real-Time Predictions using the saved model
predictions_df = model.transform(parsed_df)

# Select only the relevant columns to display in the terminal
final_output = predictions_df.select("nameOrig", "amount", "type", "prediction")

from pyspark.sql.functions import to_json, struct

print("Routing predictions to Kafka topic 'predictions_output'...")
# Kafka requires data to be a single string column named "value"
kafka_output = final_output.selectExpr("to_json(struct(*)) AS value")

query = kafka_output.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "predictions_output") \
    .option("checkpointLocation", "./checkpoint_dir") \
    .start()

query.awaitTermination()