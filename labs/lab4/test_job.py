from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

# Create a Spark Session
spark = SparkSession.builder \
    .appName("Iceberg PySpark Job") \
    .enableHiveSupport() \
    .getOrCreate()

print("Spark Session created successfully.")

schema = StructType([
    StructField("vendor_id", IntegerType(), True),
    StructField("trip_id", IntegerType(), True),
    StructField("trip_distance", FloatType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True)
])

data = [
    (1, 100, 2.5, 10.0, "Y"),
    (2, 101, 3.0, 15.0, "N"),
    (1, 102, 1.5, 8.0, "Y")
]

# Create DataFrame
df = spark.createDataFrame(data, schema)
print("DataFrame created successfully.")

path = "s3://XX/tmp2/"

# Sync with Glue Hive metastore
df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", path) \
    .saveAsTable("default.taxi")

print("Data written and table registered in Glue Hive metastore successfully.")
