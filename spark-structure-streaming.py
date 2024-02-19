from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
import pymysql
import pyspark.sql.functions as f

def insert_into_phpmyadmin(row):
    # Define the connection details for your PHPMyAdmin database
    host = "localhost"
    port = 3306
    database = "project"
    username = "root"
    password = ""
    try:
        conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
        cursor = conn.cursor()
        # # Extract the required columns from the row
        column1_value = row.Day_of_week
        column2_value = row.total_accidents_per_day
        sql_query = f"INSERT INTO user1 (Day_of_week,accidents_per_day) VALUES ('{column1_value}','{column2_value}')"
    
        cursor.execute(sql_query)

    # Commit the changes
        conn.commit()
    except Exception as e:
        print(f"Error inserting data into MySQL: {e}")
    finally:
        conn.close()
        
        
def insert_into_phpmyadmin2(row2):
        # Define the connection details for your PHPMyAdmin database
        host = "localhost"
        port = 3306
        database = "project"
        username = "root"
        password = ""
        try:
            conn = pymysql.connect(host=host, port=port, user=username, passwd=password, db=database)
            cursor = conn.cursor()
            # # Extract the required columns from the row
            column1_value = row2.location
            column2_value = row2.total_accidents_per_location
            sql_query = f"INSERT INTO user2 (location,accidents_per_location) VALUES ('{column1_value}','{column2_value}')"
    
            cursor.execute(sql_query)

    # Commit the changes
            conn.commit()
        except Exception as e:
            print(f"Error inserting data into MySQL: {e}")
        finally:
            conn.close()
    
# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaConsumer") \
    .getOrCreate()
# define the dataset schema
spark.sparkContext.setLogLevel('WARN')
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
# Read data from Kafka topic as a DataFrame
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data"))
# select the data you want from the dataframe   
df = df.filter(col("data.Day_of_week") != "Day_of_week")
df = df.select("data.Day_of_week")
aggdf = df.groupby("Day_of_week").agg(count("Day_of_week").alias("total_accidents_per_day"))
final_df = aggdf.select("Day_of_week","total_accidents_per_day")

# Read data from Kafka topic as a DataFrame
df2 = spark.readStream \
.format("kafka") \
.option("kafka.bootstrap.servers", "localhost:9092") \
.option("subscribe", "test") \
.load() \
.select(from_json(col("value").cast("string"), schema).alias("data"))
# select the data you want from the dataframe
df2 = df2.filter(col("data.location") != "location")
df2 = df2.select("data.location")
aggdf2 = df2.groupby("location").agg(count("location").alias("total_accidents_per_location"))
final_df2 = aggdf2.select("location","total_accidents_per_location")


# Convert the value column to string and display the result
query = final_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
# Insert into MySQL using foreach
insert_query = final_df.writeStream \
    .outputMode("update") \
    .foreach(insert_into_phpmyadmin) \
    .start()
    
query2 = final_df2.writeStream \
    .outputMode("update") \
    .format("console") \
    .start()
# Insert into MySQL using foreach
insert_query2 = final_df2.writeStream \
    .outputMode("update") \
    .foreach(insert_into_phpmyadmin2) \
    .start()
# Wait for the queries to finish
query.awaitTermination()
query2.awaitTermination()
insert_query.awaitTermination()
insert_query2.awaitTermination()