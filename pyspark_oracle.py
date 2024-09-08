# Import necessary libraries for PySpark Oracle connector
from pyspark.sql import SparkSession

# Create a SparkSession with a specific application name
spark = SparkSession.builder.appName("PySpark Oracle Example").getOrCreate()

# Oracle connection details
oracle_username = "your_username"
oracle_password = "your_password"
oracle_host = "your_host"
oracle_port = 1521
oracle_service_name = "your_service_name"

# Create an Oracle connection
url = f"jdbc:oracle:thin:@{oracle_host}:{oracle_port}:{oracle_service_name}"
properties = {"user": oracle_username, "password": oracle_password, "driver": "oracle.jdbc.OracleDriver"}

# Read data from an Oracle table
df = spark.read.format("jdbc").option("url", url).option("query", "SELECT * FROM your_table_name").option("user", oracle_username).option("password", oracle_password).load()

# Display the data
print("Data from Oracle table:")
df.show()
