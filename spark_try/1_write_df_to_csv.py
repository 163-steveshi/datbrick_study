import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("read_csv").getOrCreate()

schema = StructType(
    [
        StructField("ID", IntegerType(), True),
        StructField("AGE", IntegerType(), True),
        StructField("NAME", StringType(), True),
    ]
)

path = os.path.abspath("../data/01.csv")
df = spark.read.option("header", "true").schema(schema).csv(path)

df.printSchema()
df.show()
new_data = [(123, 18, "JOHN DOLL"), (321, 28, "JANE DOE")]

df_new_data = spark.createDataFrame(new_data, schema=schema)
df_combined = df.unionByName(df_new_data)

df_combined.show()

df_combined.select("NAME").show()

spark.stop()
