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

# GCS credentials from local env variables
GCS_PROJECT_ID = os.environ["GCS_PROJECT_ID"]
GCS_SERVICE_ACCOUNT_KEY_PATH = os.environ[
    "GCS_SERVICE_ACCOUNT_KEY_PATH"
]  # Path to your JSON key file
GCS_BUCKET = os.environ["GCS_BUCKET"]

spark = (
    SparkSession.builder.appName("gcs_reader")
    .master("local[*]")
    # GCS connector JAR — use the shaded version to avoid dependency conflicts
    # com.google.common is part of Guava — the GCS connector needs a newer version of Guava than what Spark ships with. They're clashing.
    # The fix — switch to the shaded version of the connector, which bundles its own Guava internally and avoids the conflict:
    .config(
        "spark.jars",
        "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.22/gcs-connector-hadoop3-2.2.22-shaded.jar",
    )
    # Tell Hadoop to use the GCS filesystem implementation
    .config(
        "spark.hadoop.fs.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    )
    .config(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    )
    # Auth: service account JSON key file
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    .config(
        "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
        GCS_SERVICE_ACCOUNT_KEY_PATH,
    )
    # GCP project
    .config("spark.hadoop.fs.gs.project.id", GCS_PROJECT_ID)
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


df = spark.read.schema(schema).csv(
    f"gs://{GCS_BUCKET}/transaction_200.csv", header=True
)
df.show(5)


spark.sql("CREATE DATABASE IF NOT EXISTS gcp_demo_db")

# step as the save as table
# check db existed
# Register table in metastore
# Decide if table is managed or external
# Write metadata

df.write.mode("overwrite").option(
    "path",
    f"gs://{GCS_BUCKET}/pos_transaction.parquet",
).saveAsTable("gcp_demo_db.tbl_customers")


rs = spark.sql(
    "SELECT * FROM gcp_demo_db.tbl_customers WHERE payment_type = 'Credit Card'"
)

rs.show()

df = spark.read.schema(schema).parquet(f"gs://{GCS_BUCKET}/pos_transaction.parquet")
df.show(10)

spark.stop()
