import os
from pyspark.sql import SparkSession

# Snowflake connection details
    spark = SparkSession.builder.appName('Snowflake Spark Example').getOrCreate()
    try:
        # Load Snowflake Spark connector
        spark._jvm.net.snowflake.spark.snowflake

        # Write to Snowflake
        df = spark.createDataFrame([(1, 'John'), (2, 'Mary')], ['id', 'name'])
        df.write.format('snowflake').options({
            'sf_url': f'https://{os.environ["SF_ACCOUNT"]}.snowflakecomputing.com/',
            'sf_user': os.environ["SF_USER"],
            'sf_password': os.environ["SF_PASSWORD"],
            'sf_warehouse': os.environ["SF_WAREHOUSE"],
            'sf_database': os.environ["SF_DB"],
            'sf_schema': os.environ["SF_SCHEMA"]
        }).option('dbtable', 'my_table').save()

        # Read from Snowflake
        df = spark.read.format('snowflake').options({
            'sf_url': f'https://{os.environ["SF_ACCOUNT"]}.snowflakecomputing.com/',
            'sf_user': os.environ["SF_USER"],
            'sf_password': os.environ["SF_PASSWORD"],
            'sf_warehouse': os.environ["SF_WAREHOUSE"],
            'sf_database': os.environ["SF_DB"],
            'sf_schema': os.environ["SF_SCHEMA"]
        }).option('dbtable', 'my_table').load()
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
import os
