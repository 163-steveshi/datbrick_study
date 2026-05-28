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

spark = SparkSession.builder.appName("two_json").master("local[*]").getOrCreate()

# set just in case
spark.conf.set("spark.sql.session.timeZone", "UTC")

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

path = os.path.abspath("../data/trans/transaction_200.csv")
df = spark.read.option("header", "true").schema(schema).csv(path)

# df.printSchema()
# df.show(6)

nljson_output_dir = os.path.abspath("../data/trans/nl_json_200")

df.write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS").json(
    nljson_output_dir
)
df_nljson = spark.read.json(nljson_output_dir)
df_nljson.show(5)

normal_json_output_dir = os.path.abspath("../data/trans/normal_json_200")
# df.write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS").option(
#     "multiline", True
# ).json(normal_json_output_dir)
# or write to the nested way
write_options = {"timestampFormat": "yyyy-MM-dd HH:mm:ss.SSS", "multiline": "true"}

# multiLine is a read option only, Spark silently ignores it on write.
# Spark cannot write pretty printed JSON natively, period. No option exists to change that behavior. Output will always be NDJSON regardless of what write options you pass.

df.write.mode("overwrite").options(**write_options).json(normal_json_output_dir)
df_normal_json = spark.read.json(normal_json_output_dir)
df_normal_json.show(5)


parquet_output_dir = os.path.abspath("../data/trans/parquet_json_200")
df.write.mode("overwrite").option("timestampFormat", "yyyy-MM-dd HH:mm:ss.SSS").parquet(
    parquet_output_dir
)

df_parquet = spark.read.parquet(parquet_output_dir)
df_parquet.show(5)
spark.stop()
