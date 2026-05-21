"""
Diagnostic script to test PySpark connectivity and configuration.
"""

import os
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("\n" + "="*80)
print("PYSPARK DIAGNOSTIC REPORT")
print("="*80 + "\n")

# Check Python version
print(f"[OK] Python Version: {sys.version}")

# Check environment variables
print(f"\nEnvironment Variables:")
print(f"  - JAVA_HOME: {os.environ.get('JAVA_HOME', 'NOT SET')}")
print(f"  - SPARK_HOME: {os.environ.get('SPARK_HOME', 'NOT SET')}")

# Try importing PySpark
try:
    import pyspark
    print(f"[OK] PySpark Version: {pyspark.__version__}")
except Exception as e:
    print(f"[ERROR] PySpark Import Error: {e}")
    sys.exit(1)

# Try creating Spark session with minimal config
try:
    from pyspark.sql import SparkSession
    print("\nAttempting to create Spark session...")
    
    spark = SparkSession.builder \
        .appName("DiagnosticTest") \
        .master("local[1]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    
    print("[OK] Spark session created successfully!")
    print(f"  - Spark Version: {spark.version}")
    print(f"  - Master: {spark.sparkContext.master}")
    
    # Test basic operation
    data = [(1, "test")]
    df = spark.createDataFrame(data, ["id", "value"])
    result = df.collect()
    
    print(f"[OK] Basic operation successful: {result}")
    spark.stop()
    print("[OK] Spark session stopped successfully")
    
except Exception as e:
    print(f"[ERROR] Spark Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*80)
print("SUCCESS: All diagnostics passed! Ready to run analytics.")
print("="*80 + "\n")
