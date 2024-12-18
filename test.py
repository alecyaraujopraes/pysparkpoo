from pyspark.sql import SparkSession


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

file_path = "input_data.json"

spark_df = spark.read.json(file_path)

print(spark_df)

