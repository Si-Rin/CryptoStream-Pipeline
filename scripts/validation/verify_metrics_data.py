# verify_metrics_data.py
# This script is used to verify the global metrics data ingested into the Delta Lake. 
# It reads the data from the specified Delta Lake path and prints the contents along with the row count.
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
import os

DELTA_PATH = os.path.abspath("data/bronze/global_metrics")

builder = SparkSession.builder.appName("verify") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.read.format("delta").load(DELTA_PATH).show()
print("Row count:", spark.read.format("delta").load(DELTA_PATH).count())