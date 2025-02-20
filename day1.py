from pyspark.sql import SparkSession
import time

# Create a SparkSession
spark = SparkSession.builder.appName("MySparkApp").getOrCreate()


# Read JSON with schema
file_read = spark.read.option("header", True).option("inferSchema", True).csv("./google_data_category.csv")

# Apply DataFrame transformations
filtered_df = file_read.filter(file_read.reviewsCount > 2)
filtered_df.select("name", "searchString" ,"city", "totalScore", "reviewsCount").show()


# Stop Spark session
spark.stop()
