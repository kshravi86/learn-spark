# Import necessary libraries for Spark Structured Streaming
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split  # Import functions for data manipulation

# Create a SparkSession with a specific application name
spark = SparkSession.builder.appName("Structured Streaming Example").getOrCreate()

# Read a stream from a Kafka topic with specific bootstrap server and topic
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "my_topic") \
    .load()  # Load the stream from Kafka

# Print the schema of the stream
print("Stream Schema:")
print(df.schema)

# Apply some transformations to the stream
# Let's assume we want to count the number of words in each language
df_transformed = df.select(col("value").cast("string")) \
    .select(explode(split(col("value"), ",")).alias("word")) \
    .withColumn("language", col("word").substr(0, 2)) \
    .groupBy("language") \
    .agg({"word": "count"})

# Write the transformed stream to the console
query = df_transformed.writeStream.outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
