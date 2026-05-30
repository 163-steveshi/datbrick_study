from platform import win32_is_iot

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import (
    lit,
    count,
    max,
    min,
    when,
    col,
    trim,
    stack,
    explode,
    split,
    row_number,
    dense_rank,
)
from pyspark.sql import Window

import os

# 1. Initialize a local Spark Session
spark = (
    SparkSession.builder.appName("transformation app")
    .config("spark.sql.warehouse.dir", os.path.abspath("../data/spark-warehouse"))
    .master("local[*]")
    .getOrCreate()
)


parquet_schema = "id Int, name String, age Int, Score Int"
sample_data = [
    (1, "Alice ", 30, 85),
    (2, " Bob", 25, 90),
    (3, "Charlie", 35, 80),
    (4, "David", 28, 88),
    (5, "Eve", 22, 92),
    (6, "Frank", 40, 78),
]


df = spark.createDataFrame(schema=parquet_schema, data=sample_data)

df.show()

# cretae temporary viewin this session
df.createOrReplaceTempView("vw_customers")
rs = spark.sql("SELECT * FROM vw_customers WHERE Score > 85")
rs.show()

# write_path = os.path.abspath("data/")
# path = os.path.abspath("../data/spark-warehouse/tbl_customers")

spark.sql("CREATE DATABASE IF NOT EXISTS demo_db")
# # save as parquet for distributing purppse
# df.write.mode("overwrite").option("path", path).saveAsTable("tbl_customers")


# df.write.mode("overwrite").option("path", "spark-warehouse").saveAsTable(
#     "demo_db.tbl_customers"
# )


df.write.mode("overwrite").option(
    "path", os.path.abspath("../data/spark-warehouse/demo_db/tbl_customers")
).saveAsTable("demo_db.tbl_customers")

rs = spark.sql("SELECT * FROM demo_db.tbl_customers WHERE Score > 85")
rs.show()

df = spark.read.parquet(
    os.path.abspath("../data/spark-warehouse/demo_db/tbl_customers")
)

# df = spark.read.parquet("spark-warehouse/demo_db.db/spark-warehouse")

df.show()

spark.stop()
