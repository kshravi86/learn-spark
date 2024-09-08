from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession with the application name "Postgres Connector"
spark = SparkSession.builder.appName("Postgres Connector").getOrCreate()

# Replace with your Postgres credentials
# Replace with your Postgres credentials
# These variables will be used to connect to your Postgres database
username = "your_username"
password = "your_password"
host = "your_host"
port = 5432
database = "your_database"

# Create a MySQL connection
# Create a JDBC URL for the Postgres database
# This URL will be used to connect to the database
url = f"jdbc:mysql://{host}:{port}/{database}"
# Define the properties for the JDBC connection
properties = {"user": username, "password": password, "driver": "com.mysql.cj.jdbc.Driver"}

# Read data from MySQL
# Read data from the Postgres database using the JDBC connection
# This will load the data from the specified table into a DataFrame
df = spark.read.format("jdbc").option("url", url).option("query", "SELECT * FROM your_table").option("user", username).option("password", password).load()

# Display the data
# Display the data in the DataFrame
# This will print the data to the console
df.show()

# Write data to MySQL
# Create a sample dataset to write to the Postgres database
data = [("John", 25), ("Mary", 31), ("David", 42)]
# Define the schema for the DataFrame
schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
# Create a DataFrame from the sample data
df = spark.createDataFrame(data, schema)
# Write the DataFrame to the Postgres database using the JDBC connection
# This will save the data to the specified table
df.write.format("jdbc").option("url", url).option("dbtable", "your_table").option("user", username).option("password", password).save()
