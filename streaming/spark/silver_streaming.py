"""
Silver layer — 1-minute OHLCV candles per symbol.

Reads raw trades from Bronze Delta, deduplicates by trade_id within a
watermark, aggregates into 1-minute tumbling windows.

Watermark: 2 minutes. Late trades within 2 minutes of the end of their
window are accepted; later than that are dropped.
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))

from dotenv import load_dotenv
from spark_session import get_spark_session

from pyspark.sql.functions import (
    col, window, count, sum as F_sum, avg, max as F_max, min as F_min,
    struct, expr,
)

load_dotenv()

BRONZE_PATH       = os.getenv("BRONZE_PATH")
SILVER_PATH       = os.getenv("SILVER_PATH")
SILVER_CHECKPOINT = os.getenv("SILVER_CHECKPOINT")

WATERMARK         = "2 minutes"
WINDOW_DURATION   = "1 minute"

spark = get_spark_session()
spark.sparkContext.setLogLevel("WARN")
print(f"Silver: bronze={BRONZE_PATH} | silver={SILVER_PATH} | chk={SILVER_CHECKPOINT}")

bronze = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
    .select("symbol", "price", "quantity", "event_time", "trade_id")
)

# Deduplicate FIRST, gated by watermark to keep state bounded.
# A WebSocket reconnect on the producer side can replay the same trade_id;
# we drop the duplicate before it pollutes the OHLCV aggregation.
deduped = (
    bronze
    .withWatermark("event_time", WATERMARK)
    .dropDuplicates(["trade_id"])
)

# OHLCV per (symbol, 1-minute window).
# open  = price of trade with smallest event_time in the window
# close = price of trade with largest  event_time in the window
# We use min/max over a struct(event_time, price) to get this without
# needing orderBy (which is not allowed inside a streaming groupBy).
ohlcv = (
    deduped
    .groupBy(window(col("event_time"), WINDOW_DURATION), col("symbol"))
    .agg(
        F_min(struct(col("event_time"), col("price"))).alias("first_struct"),
        F_max(struct(col("event_time"), col("price"))).alias("last_struct"),
        F_max("price").alias("high"),
        F_min("price").alias("low"),
        F_sum("quantity").alias("volume"),
        count("*").alias("trade_count"),
        avg("price").alias("avg_price"),
        F_sum(col("price") * col("quantity")).alias("notional"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("symbol"),
        col("first_struct.price").alias("open"),
        col("high"),
        col("low"),
        col("last_struct.price").alias("close"),
        col("volume"),
        col("trade_count"),
        col("avg_price"),
        # VWAP = sum(price * qty) / sum(qty). Guard against zero-volume edge case.
        expr("CASE WHEN volume > 0 THEN notional / volume ELSE NULL END").alias("vwap"),
    )
)

query = (
    ohlcv.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CHECKPOINT)
    .option("maxFilesPerTrigger", 10)  
    .trigger(processingTime="50 seconds")
    .start(SILVER_PATH)
)

print(f"Silver streaming started. queryId={query.id}")
query.awaitTermination()