import os
import sys

# Fix pour Windows
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

from pyspark.sql import SparkSession

def get_spark_session():
    spark = (
        SparkSession.builder
        .appName("CryptoStream-Streaming")
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2")
        .config("spark.sql.streaming.checkpointLocation",
                "C:/tmp/checkpoints")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark