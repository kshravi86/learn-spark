from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create a SparkSession
spark = SparkSession.builder.appName("Spark Example").getOrCreate()

# Load a CSV file
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Handle missing values
df = df.fillna("unknown", subset=["column1", "column2"])

# Data transformation
df = df.withColumn("new_column", when(col("column1") > 10, lit("high")).otherwise("low"))

# Feature engineering
assembler = VectorAssembler(inputCols=["column1", "column2"], outputCol="features")
df = assembler.transform(df)

# Linear regression
lr = LinearRegression(featuresCol="features", labelCol="target")
model = lr.fit(df)

# Make predictions
predictions = model.transform(df)

# Save the model
model.write().overwrite().save("lr_model")

# Stop the SparkSession
spark.stop()
