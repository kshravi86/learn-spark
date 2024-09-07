from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("Postgres Connector").getOrCreate()

# Replace with your Postgres credentials
username = "your_username"
password = "your_password"
host = "your_host"
port = 5432
database = "your_database"

# Create a MySQL connection
url = f"jdbc:mysql://{host}:{port}/{database}"
properties = {"user": username, "password": password, "driver": "com.mysql.cj.jdbc.Driver"}

# Read data from MySQL
df = spark.read.format("jdbc").option("url", url).option("query", "SELECT * FROM your_table").option("user", username).option("password", password).load()

# Display the data
df.show()

# Write data to Postgres
data = [("John", 25), ("Mary", 31), ("David", 42)]
schema = StructType([StructField("name", StringType()), StructField("age", IntegerType())])
df = spark.createDataFrame(data, schema)
df.write.format("jdbc").option("url", url).option("dbtable", "your_table").option("user", username).option("password", password).save()
