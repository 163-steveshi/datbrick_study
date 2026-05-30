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

# 1. Initialize a local Spark Session
spark = (
    SparkSession.builder.appName("transformation app").master("local[*]").getOrCreate()
)


# parquet_schema = "id Int, name String, age Int, Score Int"
# sample_data = [
#     (1, "Alice ", 30, 85),
#     (2, " Bob", 25, 90),
#     (3, "Charlie", 35, 80),
#     (4, "David", 28, 88),
#     (5, "Eve", 22, 92),
#     (6, "Frank", 40, 78),
# ]
# df = spark.createDataFrame(sample_data, schema=parquet_schema)


# # remove leading and ending empty space
# df = df.withColumn("name", trim(df["name"]))
# df.show()


# df = df.withColumn(
#     "Score_group",
#     when(col("Score") > 90, "high").when(col("Score") > 80, "medium").otherwise("low"),
# )
# df = df.withColumn("target_score", col("Score") + 5)
# df.show()


# df_summary = df.groupBy("Score_group").agg(
#     count("*").alias("total_rows"),
#     max("Score").alias("max_score"),
#     min("Score").alias("min_score"),
# )
# df_summary.show()

# # this will create 6 * 3 = 18 new column since pivot will combine with each agg function
# # turn the age from col value into col header
# df_pivot = (
#     df.groupBy("Score_group")
#     .pivot("age")
#     .agg(
#         count("*").alias("total_rows"),
#         max("Score").alias("max_score"),
#         min("Score").alias("min_score"),
#     )
# )
# df_pivot.show()

# # unpivot turns those value into col value again
# df_unpivot = df_pivot.selectExpr("Score_group", "stack(6, \
#         '30', `30_total_rows`, `30_max_score`, `30_min_score`, \
#         '25', `25_total_rows`, `25_max_score`, `25_min_score`, \
#         '35', `35_total_rows`, `35_max_score`, `35_min_score`, \
#         '28', `28_total_rows`, `28_max_score`, `28_min_score`, \
#         '22', `22_total_rows`, `22_max_score`, `22_min_score`, \
#         '40', `40_total_rows`, `40_max_score`, `40_min_score`) \
#         as (age, total_rows, max_score, min_score)")
# df_unpivot.show()


# # a meaningful example
# # analyze the age group with three level scoe and check their distribution
# df_pivot = (
#     df.groupBy("age")
#     .pivot(
#         "Score_group", ["low", "medium", "high"]
#     )  # Without the list, Spark has to run an extra pass over your entire dataset just to discover what the distinct values of Score_group are — before it can even start the pivot. On a large table that's a whole extra job. Also it garantee the new column insert order
#     .agg(F.count("*"))
# )
# df_unpivot = df_pivot.selectExpr("age", "stack(3, \
#          `low`, `medium`, `high`) as (count)")
# df_unpivot.show()


# df_pivot.show()


## dlist for explode practice
# covner the value into group and rank them
dlist = ["1,2,3", "4,5", "6", "7,1,2,10", "11,3,13"]


df = spark.createDataFrame([(s,) for s in dlist], ["numbers"])

df.show()

# df.select("numbers", explode(split("numbers", ","))).show()
df = (
    df.withColumn("number", explode(split("numbers", ",")))
    .withColumn("number", col("number").cast("int"))
    .orderBy("number")
)

df.show()

# add a dense ranking
window = Window.partitionBy("numbers").orderBy(col("number").asc())
df = df.withColumn("ranking", dense_rank().over(window))

df.show()

spark.stop()
