# Import necessary libraries
# We import SparkSession to create a Spark session, 
# and functions from pyspark.sql to work with dataframes
# We also import VectorAssembler and LinearRegression for feature engineering and modeling
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

# Create a SparkSession with a unique app name
# We create a Spark session with a unique app name, 
# which is necessary for Spark to run
spark = SparkSession.builder.appName("Spark Example").getOrCreate()

# Load a CSV file with headers and infer schema
# We load a CSV file with headers, and Spark infers the schema
df = spark.read.csv("data.csv", header=True, inferSchema=True)

# Handle missing values by filling them with 'unknown'
# We fill missing values in columns 'column1' and 'column2' with 'unknown'
df = df.fillna("unknown", subset=["column1", "column2"])

# Data transformation: create a new column based on a condition
# We create a new column 'new_column' based on a condition: 
# if 'column1' is greater than 10, then 'high', otherwise 'low'
df = df.withColumn("new_column", when(col("column1") > 10, lit("high")).otherwise("low"))

# Feature engineering: assemble features using VectorAssembler
# We assemble features 'column1' and 'column2' into a single feature 'features'
assembler = VectorAssembler(inputCols=["column1", "column2"], outputCol="features")
df = assembler.transform(df)

# Linear regression: create a model and fit it to the data
# We create a Linear Regression model with 'features' as features and 'target' as label
# Then we fit the model to the data
lr = LinearRegression(featuresCol="features", labelCol="target")
model = lr.fit(df)

# Make predictions using the trained model
# We use the trained model to make predictions on the data
predictions = model.transform(df)

# Save the trained model to a file
# We save the trained model to a file 'lr_model' for future use
model.write().overwrite().save("lr_model")

# Stop the SparkSession to release resources
# We stop the Spark session to release resources and avoid memory leaks
spark.stop()
