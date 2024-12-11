from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType


spark = SparkSession.builder \
    .appName("Iceberg PySpark Job") \
    .enableHiveSupport() \
    .getOrCreate()

print("Spark session created successfully.")


def load_data_to_iceberg(spark,
                         df,
                         catalog_name,
                         database_name,
                         table_name,
                         partition_cols=None,
                         table_type='COW',
                         sql_query=None,
                         compression='snappy',
                         target_file_size=128 * 1024 * 1024
                         ):
    full_table_name = f"{catalog_name}.{database_name}.{table_name}"
    try:
        if sql_query is not None:
            print("IN sql_query")

            # Create a temporary view
            df.createOrReplaceTempView("temp_view")
            print("Created temp view ")
            transformed_df = spark.sql(sql_query)

            print("******transformed_df SCHEMA*********")
            transformed_df.printSchema()

        else:
            transformed_df = df

        writer = transformed_df.write.format("iceberg")

        # Set table properties based on table type
        if table_type.upper() == 'COW':
            writer = writer.option("write.format.default", "parquet")
            writer = writer.option("write.delete.mode", "copy-on-write")
            writer = writer.option("write.update.mode", "copy-on-write")
            writer = writer.option("write.merge.mode", "copy-on-write")
        elif table_type.upper() == 'MOR':
            writer = writer.option("write.format.default", "parquet")
            writer = writer.option("write.delete.mode", "merge-on-read")
            writer = writer.option("write.update.mode", "merge-on-read")
            writer = writer.option("write.merge.mode", "merge-on-read")
        else:
            raise ValueError("Invalid table_type. Must be 'COW' or 'MOR'.")

        # Set compression codec
        writer = writer.option("write.parquet.compression-codec", compression)

        # Set target file size
        writer = writer.option("write.parquet.target-file-size", target_file_size)

        if partition_cols:
            writer = writer.partitionBy(partition_cols)

        if spark.catalog.tableExists(full_table_name):
            print(f"Appending data to existing table {full_table_name}")
            writer.mode("append").saveAsTable(full_table_name)
        else:
            print(f"Creating new table {full_table_name}")
            writer.mode("overwrite").saveAsTable(full_table_name)

        print(f"Data successfully written to {full_table_name}")

        if sql_query:
            spark.catalog.dropTempView("temp_view")

        return True

    except Exception as e:
        print(f"Error loading data to Iceberg: {str(e)}")
        return False  # Return False on failure


def main():
    schema = StructType([
        StructField("vendor_id", IntegerType(), True),
        StructField("trip_id", IntegerType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True)
    ])

    print("Schema defined successfully.")

    # Create sample data
    data = [
        (1, 100, 2.5, 10.0, "Y"),
        (2, 101, 3.0, 15.0, "N"),
        (1, 102, 1.5, 8.0, "Y")
    ]

    # Create DataFrame
    df = spark.createDataFrame(data, schema)
    print("DataFrame created successfully.")

    load_data_to_iceberg(
        spark=spark,
        df=df,
        catalog_name="dev",
        database_name="default",
        table_name="taxis",
        partition_cols="processed_date",
        table_type='COW',
        sql_query="""
    SELECT 
        *,
        current_timestamp as processed_time,
        DATE_FORMAT(current_timestamp, 'yyyy-MM-dd') as processed_date
    FROM 
        temp_view
    """,
        compression='zstd',
        target_file_size=128 * 1024 * 1024  # Default to 128 MB
    )

    print("DataFrame written to Iceberg table successfully.")

    # Read from Iceberg table
    print("Reading from Iceberg table...")
    iceberg_df = spark.read.format("iceberg").load("dev.default.taxis")

    # Show table contents
    print("Showing table contents:")
    iceberg_df.show()

    # Perform a simple aggregation
    print("Performing aggregation...")
    result = iceberg_df.groupBy("vendor_id").sum("fare_amount")
    print("Showing aggregation result:")
    result.show()

    spark.stop()
    print("Spark session stopped.")


main()
