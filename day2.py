from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col
import time

# Create a SparkSession
spark = SparkSession.builder \
    .appName("BroadcastJoinExample") \
    .master("local[3]")\
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4041") \
    .getOrCreate()

# Large DataFrame
data_large = [(i, f"Customer_{i}", i % 5, i+10) for i in range(1, 1000001)]  # 1M rows
df_large = spark.createDataFrame(data_large, ["customer_id", "name", "country_id", 'salary'])

df_large = df_large.filter(col("salary") > 5000)
# Small DataFrame
data_small = [(0, "USA"), (1, "India"), (2, "Canada"), (3, "Germany"), (4, "UK")]
df_small = spark.createDataFrame(data_small, ["country_id", "country_name"])

print("Number of partitions in large dataset:", df_large.rdd.getNumPartitions())
print("Number of partitions in small dataset:", df_small.rdd.getNumPartitions())

# df_broadcast = df_large.join(broadcast(df_small), "country_id", "inner")
# df_broadcast.explain()  # No shuffling happens
# df_broadcast.show()  
# spark.conf.set("spark.sql.join.preferShuffledHashJoin", "true")
df_join = df_large.join(df_small, "country_id", "inner")
df_join.explain()  # Shows that shuffling happens
df_join.show()

# Keep UI Open
time.sleep(600)  
# Stop Spark session
spark.stop()
