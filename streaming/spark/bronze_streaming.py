"""
Bronze layer — raw trades from Kafka into Delta Lake.

Bronze is APPEND-ONLY and IMMUTABLE. We do NOT compute OHLCV here.
OHLCV candles are produced by the Silver layer via 1-minute window
aggregations over these trades.

Stored per row:
- the 6 unified trade fields from the producer
- event_time as a real Spark timestamp (was epoch ms in the Kafka payload)
- the original epoch ms preserved as event_time_ms for audit
- Kafka offset metadata (topic / partition / offset / kafka_timestamp)
- raw_payload (the original JSON string) for replay if schema ever changes
- ingested_at for late-arrival analysis
"""
import os
import sys
import signal
sys.path.append(os.path.dirname(__file__))

from dotenv import load_dotenv
from spark_session import get_spark_session

from pyspark.sql.functions import (
    col, from_json, from_unixtime, to_timestamp, current_timestamp,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType,
)

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
BRONZE_PATH       = os.getenv("BRONZE_PATH")
BRONZE_CHECKPOINT = os.getenv("BRONZE_CHECKPOINT")

# Must match the producer's unified schema exactly.
TRADE_SCHEMA = StructType([
    StructField("symbol",     StringType(), False),
    StructField("price",      DoubleType(), False),
    StructField("quantity",   DoubleType(), False),
    StructField("event_time", LongType(),   False),  # epoch ms
    StructField("trade_id",   StringType(), False),
    StructField("source",     StringType(), False),
])

spark = get_spark_session()
spark.sparkContext.setLogLevel("WARN")
print(f"Bronze: bootstrap={KAFKA_BOOTSTRAP} | path={BRONZE_PATH} | chk={BRONZE_CHECKPOINT}")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    # Explicit list — no auto-discovery, no dead-letter pickup.
    .option(
        "subscribe",
        "crypto-trades-btcusdt,crypto-trades-ethusdt,"
        "crypto-trades-xrpusdt,crypto-trades-solusdt,"
        "crypto-trades-adausdt",
    )
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 5000)
    .load()
)

parsed = (
    raw.select(                                       # was [raw.select](...)
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_payload"),
        from_json(col("value").cast("string"), TRADE_SCHEMA).alias("t"),
    )
)

bronze = (
    parsed.select(                                    # was [parsed.select](...)
        "topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
        "raw_payload",
        col("t.symbol").alias("symbol"),
        col("t.price").alias("price"),
        col("t.quantity").alias("quantity"),
        to_timestamp(from_unixtime(col("t.event_time") / 1000)).alias("event_time"),
        col("t.event_time").alias("event_time_ms"),
        col("t.trade_id").alias("trade_id"),          # was col("[t.trade](...)_id")
        col("t.source").alias("source"),
        current_timestamp().alias("ingested_at"),
    )
    .filter(col("trade_id").isNotNull())
)

query = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", BRONZE_CHECKPOINT)
    # Tell Delta to compact the log every 10 commits
    .option("delta.logRetentionDuration", "interval 1 days")
    .option("delta.checkpointInterval", "10")
    .trigger(processingTime="30 seconds")
    .start(BRONZE_PATH)
)

print(f"Bronze streaming started. queryId={query.id}")

def shutdown(sig, frame):
    print("Graceful shutdown requested — stopping query...")
    query.stop()
    spark.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

query.awaitTermination()