import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType,
)
from pyspark.sql.functions import count, min, max, when, col

spark = SparkSession.builder.appName("agg_fun_usage").master("local[*]").getOrCreate()
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

path = os.path.abspath("../data/trans/parquet_json_200")
df = spark.read.option("header", "true").schema(schema).parquet(path)


df_summary_goup = df.withColumn(
    "price_group",
    when(col("item_unit_price") > 5000, "high")
    .when(col("item_unit_price") > 1000, "medium")
    .otherwise("low"),
)

df_customer_price_range = df.groupBy(["customer_id"]).agg(
    count("*").alias("total_rows"),
    max("item_unit_price").alias("max_price"),
    min("item_unit_price").alias("min_price"),
)

df_summary = df.agg(
    count("*").alias("total_rows"),
    max("item_unit_price").alias("max_price"),
    min("item_unit_price").alias("min_price"),
)

df_summary.crossJoin(df_customer_price_range).show(5)


df.crossJoin(df_summary).show(10)


employee_schema = StructType(
    [
        StructField("emp_id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("dept_id", IntegerType(), nullable=False),
    ]
)
dept_schema = StructType(
    [
        StructField("dept_id", IntegerType(), nullable=False),
        StructField("dept_name", StringType(), nullable=False),
        StructField("budget", StringType(), nullable=False),
    ]
)
emp_data = [
    (1, "Alice", 10),
    (2, "Bob", 20),
    (3, "Carol", 10),
    (4, "Dave", 30),
    (5, "Eve", 40),
]

dept_data = [
    (10, "Engineering", "500k"),
    (20, "Marketing", "300k"),
    (40, "HR", "200k"),
]


df_emp = spark.createDataFrame(emp_data, schema=employee_schema)

df_dept = spark.createDataFrame(dept_data, schema=dept_schema)

df_emp.join(df_dept, how="left", on="dept_id").show()
# explict left join
df_emp.join(
    df_dept,
    df_emp["dept_id"] == df_dept["dept_id"],
    how="inner",
).show()

df_emp.union(df_emp).orderBy("emp_id").show()


df.crossJoin(df_summary).select(
    ["customer_id", "total_rows", "max_price", "min_price"]
).union(
    df.join(df_customer_price_range, how="left", on="customer_id").select(
        ["customer_id", "total_rows", "max_price", "min_price"]
    )
).orderBy(
    "customer_id"
).show(
    5
)
spark.stop()
