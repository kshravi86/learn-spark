from pyspark.sql import SparkSession

# Create a SparkSession with the application name "Postgres Connector"
spark = SparkSession.builder.appName("Postgres Connector").getOrCreate()

# Replace with your Postgres credentials
username = "your_username"
password = "your_password"
host = "your_host"
port = 5432
database = "your_database"

# Create a Postgres connection
url = f"jdbc:postgresql://{host}:{port}/{database}"
properties = {"user": username, "password": password, "driver": "org.postgresql.Driver"}

# Read the list of tables in the public schema
df = spark.read.format("jdbc").option("url", url).option("query", "SELECT table_name FROM information_schema.tables WHERE table_schema='public'").option("user", username).option("password", password).load()

# Display the list of tables
print("List of tables in the public schema:")
df.show()
# This script connects to a Postgres database, reads the list of tables in the public schema, and prints the list to the console.
