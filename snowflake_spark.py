# Import necessary libraries for Snowflake Spark connector
import os
from pyspark.sql import SparkSession

# Snowflake connection details
sf_account = os.environ["SF_ACCOUNT"]
sf_user = os.environ["SF_USER"]
sf_password = os.environ["SF_PASSWORD"]
sf_warehouse = os.environ["SF_WAREHOUSE"]
sf_database = os.environ["SF_DB"]
sf_schema = os.environ["SF_SCHEMA"]

# Create a SparkSession with a specific application name
spark = SparkSession.builder.appName('Snowflake Spark Example').getOrCreate()

# Load Snowflake Spark connector
spark._jvm.net.snowflake.spark.snowflake

# Read all tables from the public schema in Snowflake
tables_df = spark.read.format('snowflake').options({
    'sf_url': f'https://{sf_account}.snowflakecomputing.com/',
    'sf_user': sf_user,
    'sf_password': sf_password,
    'sf_warehouse': sf_warehouse,
    'sf_database': sf_database,
    'sf_schema': sf_schema
}).option('query', 'SHOW TABLES IN public').load()

# Print the list of tables
print("Tables in the public schema:")
tables_df.show()

# Iterate over each table and print its columns
for row in tables_df.collect():
    table_name = row['TABLE_NAME']
    print(f"Columns in table {table_name}:")
    columns_df = spark.read.format('snowflake').options({
        'sf_url': f'https://{sf_account}.snowflakecomputing.com/',
        'sf_user': sf_user,
        'sf_password': sf_password,
        'sf_warehouse': sf_warehouse,
        'sf_database': sf_database,
        'sf_schema': sf_schema
    }).option('query', f'DESCRIBE TABLE public.{table_name}').load()
    columns_df.show()
