import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType,
)

load_dotenv()

# Azure credentials from local env variables
AZURE_STORAGE_ACCOUNT = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_STORAGE_KEY = os.environ["AZURE_STORAGE_KEY"]
AZURE_CONTAINER = os.environ["AZURE_CONTAINER"]

spark = (
    SparkSession.builder.appName("azure_blob_reader")
    .master("local[*]")
    .config(
        f"spark.hadoop.fs.azure.account.key.{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net",
        AZURE_STORAGE_KEY,
    )
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-azure:3.3.4,"
        "com.microsoft.azure:azure-storage:8.6.6",
    )
    .config(
        "spark.hadoop.fs.azure.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
    )
    .getOrCreate()
)

schema = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("item_sequence", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("item_unit_price", DecimalType(10, 2), True),
        StructField("item_quantity", IntegerType(), True),
        StructField("payment_type", StringType(), True),
        StructField("country", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("hour_of_day", IntegerType(), True),
        StructField("is_weekend", IntegerType(), True),
        StructField("is_noise_row", IntegerType(), True),
    ]
)

# wasbs:// for Blob Storage (like s3a:// for S3)
azure_uri = f"abfss://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/transaction_200.csv"


df = spark.read.schema(schema).csv(azure_uri, header=True)
df.show(5)


spark.sql("CREATE DATABASE IF NOT EXISTS azure_demo_db")

# step as the save as table
# check db existed
# Register table in metastore
# Decide if table is managed or external
# Write metadata

df.write.mode("overwrite").option(
    "path",
    # cannot use wasbs: azure cannot use spark temporary name
    f"abfss://{AZURE_CONTAINER}@{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net/pos_transaction.parquet",
).saveAsTable("azure_demo_db.tbl_customers")

# blob.core.windows.net → “object storage API”
# No real directory operations
# No atomic rename of folders
# Slow directory-like operations
# Limited big data compatibility

# dfs.core.windows.net → “data lake (file system) API”
# Real hierarchical filesystem
# True directories exist
# Supports folder operations


rs = spark.sql(
    "SELECT * FROM azure_demo_db.tbl_customers WHERE payment_type = 'Credit Card'"
)

rs.show()
spark.stop()
