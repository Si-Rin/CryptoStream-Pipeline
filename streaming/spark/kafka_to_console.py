"""
Spark Structured Streaming smoke test:
  Kafka (crypto-trades-*) → parse unified trade JSON → console sink.

This is the Day-2 acceptance check for the trade schema (NOT OHLCV).
Run it AFTER producer_ws.py is producing.
"""
import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

# Unified trade schema produced by ingestion/producer_ws.py
TRADE_SCHEMA = StructType([
    StructField("symbol",     StringType(), False),
    StructField("price",      DoubleType(), False),
    StructField("quantity",   DoubleType(), False),
    StructField("event_time", LongType(),   False),  # epoch ms
    StructField("trade_id",   StringType(), False),
    StructField("source",     StringType(), False),
])

spark = (
    SparkSession.builder
    .appName("CryptoStream-KafkaToConsole")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2",
    )
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribePattern", "crypto-trades-.*")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

trades = (
    raw.select(
        col("topic"),
        col("partition"),
        col("offset"),
        from_json(col("value").cast("string"), TRADE_SCHEMA).alias("t"),
    )
    .select("topic", "partition", "offset", "t.*")
    .withColumn("event_ts", to_timestamp(from_unixtime(col("event_time") / 1000)))
)

query = (
    trades.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("numRows", 20)
    .trigger(processingTime="5 seconds")
    .start()
)

query.awaitTermination()