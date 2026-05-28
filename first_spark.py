from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import time

# 1. Initialize a local Spark Session
spark = (
    SparkSession.builder.appName("LocalSanityCheck").master("local[*]").getOrCreate()
)


# dont use inferSchema if file is large, it can be time consuming.
# Instead, specify the schema explicitly or use a smaller sample of the data for inference.


production_schema = StructType(
    [StructField("Topic", StringType(), True), StructField("Year", IntegerType(), True)]
)
# Define schema using a DDL-formatted string
# ddl_schema = "name STRING, age INT, is_active BOOLEAN" (fragile in production when have nested object)


df_from_csv = (
    spark.read.option("header", "true").schema(production_schema).csv("data/input.csv")
)

# 2. Create a minimal dataset (Python list of tuples)
data = [("Data Engineering", 2026), ("Agentic AI", 2026)]
columns = ["Topic", "Year"]

# 3. Create a DataFrame
df = spark.createDataFrame(data, schema=columns)

df_combined = df_from_csv.unionByName(df)


# 4. Perform a simple transformation and display the output
print("\n" + "=" * 40 + "\nSPARK SUCCESSFUL RUN:\n" + "=" * 40)
df_combined.select("Topic").show()

print("\nWriting DataFrame out to CSV directory...")

# dont do coalese if the dataset is large, it can lead to performance issues.
# Instead, write the DataFrame in its default partitioned format or specify a reasonable number of partitions based on the size of the data.
# df_combined.coalesce(1).write.mode("overwrite").option("header", "true").csv("data\\output_data")
df_combined.write.mode("overwrite").option("header", "true").csv("data_output_data")


time.sleep(50)

# 5. Clean up and close the session
spark.stop()
