from ast import alias
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
from pyspark.sql.functions import lit, count, min, max, when, col

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


# add new column: with column ===> alter table  x add column y
df = df.withColumn("tag", lit(1))
df.show(20)

# agg function usage summary the tbale

df_summary = df.agg(
    count("*").alias("total_rows"),
    min("item_unit_price").alias("min_price"),
    max("item_unit_price").alias("max_price"),
)
df_summary.show(5)

# case when then
df = df.withColumn(
    "price_group",
    when(col("item_unit_price") > 5000, "high")
    .when(col("item_unit_price") > 1000, "medium")
    .otherwise("low"),
)

df.show(5)

# add columm value to creaet a new colun
df = df.withColumn(
    "hash_secret", col("tag") + col("is_noise_row") + col("is_weekend")
).alias(
    "secret_flag"
)  # not work for the alias since this is dataframe level and also has the withCOLUMN

# need to be on the columnn
df.select((col("tag") + col("is_noise_row") + col("is_weekend")).alias("secret_flag"))
df.show(5)

# group by usage check customer spend
df_summary = (
    df.groupBy(["price_group", "customer_id"])
    .agg(
        count("*").alias("total_rows"),
        min("item_unit_price").alias("min_price"),
        max("item_unit_price").alias("max_price"),
    )
    .filter(col("min_price") > 35)
)  # pyspark synatx  not support having only filter, u need spark sql to use having
df_summary.show(10)
spark.stop()
