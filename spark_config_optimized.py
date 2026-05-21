"""
Optimized PySpark Configuration for Laptop Environment
========================================================

Hardware Specs:
  - RAM: 16GB
  - CPU Cores: 8
  - Dataset: 1 million e-commerce orders

This module provides an optimized SparkSession configuration with detailed
explanations of each setting for your specific hardware constraints.

Author: Analytics Team
Date: 2026-05-21
"""

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_optimized_spark_session(app_name: str = "OptimizedEcommerceAnalytics") -> SparkSession:
    """
    Create an optimized SparkSession for laptop with 16GB RAM and 8 CPU cores.
    
    CONFIGURATION STRATEGY:
    ========================
    For 16GB laptop processing 1M orders:
    - Reserve 8GB for OS and background processes
    - Allocate 8GB (6GB driver + 2GB executor overhead)
    - Use 8 cores for parallelism (local[8])
    
    Args:
        app_name (str): Name of the Spark application.
    
    Returns:
        SparkSession: Optimized Spark session ready for e-commerce analytics.
    
    Example:
        >>> spark = create_optimized_spark_session("MyAnalytics")
        >>> df = spark.read.parquet("orders.parquet")
    """
    
    logger.info(f"Creating optimized Spark session: {app_name}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
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
    
    logger.info("=" * 80)
    logger.info("SPARK SESSION CREATED WITH OPTIMIZED CONFIGURATION")
    logger.info("=" * 80)
    logger.info(f"Spark Version: {spark.version}")
    logger.info(f"Master: {spark.sparkContext.master}")
    logger.info(f"App Name: {spark.sparkContext.appName}")
    logger.info(f"Default Parallelism: {spark.sparkContext.defaultParallelism}")
    logger.info(f"Default Min Partitions: {spark.sparkContext.defaultMinPartitions}")
    logger.info("=" * 80)
    
    return spark


# ============ QUICK REFERENCE CONFIGURATION ============
#
# For your 16GB laptop with 8 cores processing 1M e-commerce orders:
#
# KEY SETTINGS:
# ├─ Driver Memory:           6GB (largest safe allocation)
# ├─ Executor Memory:         2GB (per executor)
# ├─ Shuffle Partitions:      64 (optimal for 8 cores)
# ├─ Parallelism:             8 (match CPU cores)
# ├─ Adaptive Execution:      Enabled (auto-optimize)
# ├─ Partition Coalescing:    Enabled (reduce overhead)
# ├─ Serializer:              Java (stable)
# ├─ Broadcast Threshold:     50MB (optimize joins)
# └─ Compression:             Snappy (good ratio/speed)
#
# EXPECTED PERFORMANCE:
# ├─ Load 1M orders:          2-3 seconds
# ├─ GroupBy aggregation:     0.5-1 second
# ├─ Join 3 tables:           1-2 seconds
# ├─ Total analytics job:     5-10 seconds
# └─ Memory used:             ~3-4GB of 6GB allocated
#
# OPTIMIZATION TIPS:
# 1. Cache frequently used tables: df.cache()
# 2. Check DAG visualization in Spark UI: http://localhost:4040
# 3. Profile with: df.explain(extended=True)
# 4. Use show(truncate=False) to see full output
# 5. Monitor with: spark.sparkContext.getExecutorMemoryStatus()


def get_spark_config_summary(spark: SparkSession) -> dict:
    """
    Get a summary of the current Spark configuration.
    
    Args:
        spark: Active SparkSession
    
    Returns:
        dict: Configuration parameters and values
    """
    config = {
        "driver_memory": spark.conf.get("spark.driver.memory"),
        "executor_memory": spark.conf.get("spark.executor.memory"),
        "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
        "adaptive_enabled": spark.conf.get("spark.sql.adaptive.enabled"),
        "coalesce_enabled": spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled"),
        "serializer": spark.conf.get("spark.serializer"),
        "parallelism": spark.sparkContext.defaultParallelism,
        "cores": spark.sparkContext.defaultParallelism,
    }
    return config


if __name__ == "__main__":
    # Example usage
    spark = create_optimized_spark_session("LaptopOptimized")
    
    # Get configuration summary
    config = get_spark_config_summary(spark)
    
    print("\nCURRENT SPARK CONFIGURATION:")
    print("=" * 60)
    for key, value in config.items():
        print(f"  {key:.<40} {value}")
    print("=" * 60)
    
    # Test with sample data
    print("\nTesting with sample e-commerce data...")
    data = [
        (1, "Electronics", 1000),
        (2, "Books", 50),
        (3, "Electronics", 800),
        (4, "Clothing", 120),
        (5, "Books", 45),
    ]
    
    df = spark.createDataFrame(data, ["order_id", "category", "amount"])
    print("\nSample DataFrame:")
    df.show()
    
    print("\nGroupBy aggregation:")
    df.groupBy("category").agg({"amount": "sum"}).show()
    
    spark.stop()
    print("\nSpark session stopped.")
