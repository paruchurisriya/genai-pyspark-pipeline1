"""
MINIMAL COPY-PASTE PYSPARK CONFIGURATION
For 16GB Laptop, 8 CPU Cores, 1M E-commerce Orders

Just copy and paste this into your project!
"""

from pyspark.sql import SparkSession

# ============ COPY THIS CODE ============

spark = SparkSession.builder \
    .appName("OptimizedEcommerceAnalytics") \
    .master("local[8]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "64") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.dynamicPartitionPruning.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer") \
    .config("spark.io.compression.codec", "snappy") \
    .config("spark.task.cpus", "1") \
    .config("spark.default.parallelism", "8") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.sql.inMemoryColumnarStorage.compressed", "true") \
    .config("spark.sql.autoBroadcastJoinThreshold", "52428800") \
    .config("spark.network.timeout", "600") \
    .config("spark.executor.heartbeatInterval", "60") \
    .getOrCreate()

# ============ END COPY-PASTE ============

# Now use it:
print(f"Spark Version: {spark.version}")
print(f"Master: {spark.sparkContext.master}")

# Example: Load and analyze data
# df = spark.read.parquet("data/raw/orders.parquet")
# df.show()

# spark.stop()
