from pyspark.sql import SparkSession

# Initialize a local Spark Session
spark = (
    SparkSession.builder.appName("WSL-Local-Testing").master("local[*]").getOrCreate()
)

# Create dummy data
data = [("Data Engineering", 1), ("AI Pipelines", 2), ("Analytics Architecture", 3)]
schema = ["Domain", "Priority"]

df = spark.createDataFrame(data, schema=schema)
df.show()

# Save DataFrame to a local CSV directory
output_path = "/home/sshi/databrick_practice/output_csv"

df.write.mode("overwrite").option("header", "true").csv(output_path)

# Stop the session
spark.stop()
