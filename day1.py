from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, spark_partition_id
import time

# Create a SparkSession
spark = SparkSession.builder \
    .appName("TestSparkUI") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# Read CSV file
file_read = spark.read.option("header", True).option("inferSchema", True).csv("./google_data_category.csv")

# Apply Transformations
filtered_df = file_read.filter(file_read.reviewsCount > 2)

# Repartition 
filtered_df = filtered_df.repartition(5) # repartitons

# Print number of partitions
print("Number of partitions in repartions:", filtered_df.rdd.getNumPartitions())

# Add partition ID column and count records per partition
partition_counts_df = filtered_df.withColumn("partitionId", spark_partition_id()) \
                                 .groupBy("partitionId") \
                                 .count()

# Show results
partition_counts_df.show()

filtered_df = filtered_df.coalesce(3)   # coalesce

# Print number of partitions
print("Number of partitions in coalesce:", filtered_df.rdd.getNumPartitions())

# Add partition ID column and count records per partition
partition_counts_df = filtered_df.withColumn("partitionId", spark_partition_id()) \
                                 .groupBy("partitionId") \
                                 .count()

# Show results
partition_counts_df.show()
# Perform Actions (Triggers Jobs)
filtered_df = filtered_df.select("name", "searchString", "city", "totalScore", "reviewsCount")

grouped_df = (
    filtered_df.groupBy("city")
    .agg(
        count("name").alias("total_places"),
        avg("totalScore").alias("avg_score"),
        avg("reviewsCount").alias("avg_reviews")
    )
)

grouped_df.show()


# Keep UI Open
time.sleep(600)  
# Stop Spark session
spark.stop()
