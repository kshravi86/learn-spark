# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create a SparkSession with a unique app name
spark = SparkSession.builder.appName("Spark Example").getOrCreate()

# Load a CSV file with headers and infer schema
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Handle missing values by filling them with 'unknown'
df = df.fillna("unknown", subset=["column1", "column2"])

# Data transformation: create a new column based on a condition
df = df.withColumn("new_column", when(col("column1") > 10, lit("high")).otherwise("low"))

# Feature engineering: assemble features using VectorAssembler
assembler = VectorAssembler(inputCols=["column1", "column2"], outputCol="features")
df = assembler.transform(df)

# Linear regression: create a model and fit it to the data
lr = LinearRegression(featuresCol="features", labelCol="target")
model = lr.fit(df)

# Make predictions using the trained model
predictions = model.transform(df)

# Save the trained model to a file
model.write().overwrite().save("lr_model")

# Stop the SparkSession to release resources
spark.stop()
