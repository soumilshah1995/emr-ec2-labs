from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Create a Spark Session
spark = SparkSession.builder.appName('ReadAndProcessDataFrame').getOrCreate()

print("Spark Session created successfully.")

# Define the schema
schema = StructType([
    StructField("c_custkey", IntegerType(), True),
    StructField("c_name", StringType(), True),
    StructField("c_address", StringType(), True),
    StructField("c_nationkey", IntegerType(), True),
    StructField("c_phone", StringType(), True),
    StructField("c_acctbal", DoubleType(), True),
    StructField("c_mktsegment", StringType(), True),
    StructField("c_comment", StringType(), True)
])

print("Schema defined successfully.")

# Read the JSON data from S3
df = spark.read.parquet("s3://XXX/raw/", schema=schema)

print("parquet data read from S3 successfully.")

# Create a temporary view
df.createOrReplaceTempView("customers")

print("Temporary view 'customers' created successfully.")

# Execute SQL query to count by c_nationkey
result_df = spark.sql("""
    SELECT 
        c_nationkey,
        COUNT(*) AS count
    FROM customers
    GROUP BY c_nationkey
""")

print("SQL query executed successfully.")

# Print the result DataFrame
result_df.show(truncate=False)

print("Result DataFrame displayed successfully.")

path = "s3://XXX/processed/nationkey_count"

# Write the result DataFrame to a Parquet file
result_df.write.parquet(path, mode="overwrite")

print("Result DataFrame written to Parquet file successfully.")
print("Complete write....... ")

# Stop the Spark Session
spark.stop()

print("Spark Session stopped successfully.")
