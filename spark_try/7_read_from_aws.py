import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Load environment variables from .env file
load_dotenv()


# S3 credentials from local env variables
AWS_S3_ACCESS_KEY = os.environ["AWS_S3_ACCESS_KEY"]
AWS_S3_SECRET_KEY = os.environ["AWS_S3_SECRET_KEY"]
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
S3_BUCKET_PATH = os.environ["S3_BUCKET_PATH"]


spark = (
    SparkSession.builder.appName("s3_parquet_reader")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.access.key", AWS_S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", AWS_S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.connection.maximum", "100")
    .config("fs.s3a.threads.max", "20")
    .getOrCreate()
)

## NOTICE: legacy code base prefer below pattern, apply setting after the session is executed
# hadoop_conf = spark._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# hadoop_conf.set("fs.s3a.access.key", s3a_access_key)
# hadoop_conf.set("fs.s3a.secret.key", s3a_secret_key)

# # 3. Performance Tuning Options
# hadoop_conf.set("fs.s3a.connection.maximum", "100")
# hadoop_conf.set("fs.s3a.threads.max", "20")


# schema = StructType(
#     [
#         StructField("ID", IntegerType(), True),
#         StructField("AGE", IntegerType(), True),
#         StructField("NAME", StringType(), True),
#     ]
# )

# df = spark.read.schema(schema).csv(f"{S3_BUCKET_PATH}/customer_test.csv", header=True)
# df.show()


# df = spark.read.parquet(f"{S3_BUCKET_PATH}/parquet_order/parquet_json_200/")
# df.show(5)

# # now write a parquet to the remote s3

df = spark.read.parquet(
    os.path.abspath("../data/spark-warehouse/demo_db/tbl_customers")
)


spark.sql("CREATE DATABASE IF NOT EXISTS s3_demo_db")

# step as the save as table
# check db existed
# Register table in metastore
# Decide if table is managed or external
# Write metadata

df.write.mode("overwrite").option(
    "path", f"{S3_BUCKET_PATH}/customers.parquet"
).saveAsTable("s3_demo_db.tbl_customers")


rs = spark.sql("SELECT * FROM s3_demo_db.tbl_customers WHERE age > 25")
rs.show()
spark.stop()
