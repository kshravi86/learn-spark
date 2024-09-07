# Import necessary libraries
from pyspark.sql import SparkSession

# Create a SparkSession with a unique app name
spark = SparkSession.builder.appName("Oracle Example").getOrCreate()

# Load Oracle JDBC driver
spark.sparkContext.addJar("ojdbc8.jar")

# Read data from Oracle database
df = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//localhost:1521/ORCLPDB1") \
    .option("query", "SELECT * FROM MY_TABLE") \
    .option("user", "my_username") \
    .option("password", "my_password") \
    .load()

# Display the data
df.show()

# Stop the SparkSession to release resources
spark.stop()
