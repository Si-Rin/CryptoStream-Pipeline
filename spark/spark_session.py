import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["HADOOP_HOME"] = "C:\\hadoop"

from pyspark.sql import SparkSession

def get_spark_session():
    spark = (
        SparkSession.builder
        .appName("CryptoStream")
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.3.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark