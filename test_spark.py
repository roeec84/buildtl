
import os
import sys
from pyspark.sql import SparkSession

os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

try:
    print("Starting Spark Session...")
    spark = SparkSession.builder \
        .appName("TestApp") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    print("Spark Session created successfully!")
    spark.stop()
except Exception as e:
    print(f"Failed: {e}")
