from pyspark.sql import SparkSession

# Snowflake connection details
SF_ACCOUNT = 'your_account_name'
SF_USER = 'your_username'
SF_PASSWORD = 'your_password'
SF_WAREHOUSE = 'your_warehouse_name'
SF_DB = 'your_database_name'
SF_SCHEMA = 'your_schema_name'

# Create a SparkSession
spark = SparkSession.builder.appName('Snowflake Spark Example').getOrCreate()

# Load Snowflake Spark connector
spark._jvm.net.snowflake.spark.snowflake

# Write to Snowflake
df = spark.createDataFrame([(1, 'John'), (2, 'Mary')], ['id', 'name'])
df.write.format('snowflake').options({
    'sf_url': f'https://{SF_ACCOUNT}.snowflakecomputing.com/',
    'sf_user': SF_USER,
    'sf_password': SF_PASSWORD,
    'sf_warehouse': SF_WAREHOUSE,
    'sf_database': SF_DB,
    'sf_schema': SF_SCHEMA
}).option('dbtable', 'my_table').save()

# Read from Snowflake
df = spark.read.format('snowflake').options({
    'sf_url': f'https://{SF_ACCOUNT}.snowflakecomputing.com/',
    'sf_user': SF_USER,
    'sf_password': SF_PASSWORD,
    'sf_warehouse': SF_WAREHOUSE,
    'sf_database': SF_DB,
    'sf_schema': SF_SCHEMA
}).option('dbtable', 'my_table').load()
