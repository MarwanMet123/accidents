from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Define the schema for your DataFrame
schema = StructType().add("time", StringType())\
                     .add("Day_of_week", StringType())\
                     .add("Sex_of_driver", StringType())\
                     .add("Type_of_vehicle", StringType())\
                     .add("location", StringType())\
                     .add("road_conditin", StringType())\
                     .add("Weather_conditions", StringType())\
                     .add("Type_of_collision", StringType())\
                     .add("Cause_of_accident", StringType())\
                     .add("Accident_severity", StringType())

# Read data from a directory as a streaming DataFrame
streaming_df = spark.readStream \
    .format("csv") \
    .schema(schema) \
    .option("path", r"D:\Project\bd\data") \
    .load() \

df = streaming_df.select(to_json(struct("*")).alias("value"))

# Convert the value column to string and display the result
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "test") \
    .option("checkpointLocation", "null") \
    .start()

# Wait for the query to finish
query.awaitTermination()
