# bronze_vs_silver_check.py
from streaming.spark.spark_session import get_spark_session
from pyspark.sql.functions import col, count, sum as F_sum, min as F_min, max as F_max

spark = get_spark_session()
bronze = spark.read.format("delta").load("./data/bronze/trades")
silver = spark.read.format("delta").load("./data/silver/ohlcv_1m")

# Pick one closed window — say, 3 minutes ago — and validate it manually.
target_window = "2026-04-25 15:18:00"  # set to a real closed minute

print("=== BRONZE recomputation for window starting", target_window, "===")
b = bronze.filter(
    (col("event_time") >= target_window) &
    (col("event_time") < f"{target_window[:-2]}{int(target_window[-2:])+1:02d}")  # +1min
).filter(col("symbol") == "BTCUSDT")

# Manual OHLCV from raw trades, deduped on trade_id
b_dedup = b.dropDuplicates(["trade_id"])
b_dedup.agg(
    count("*").alias("trade_count"),
    F_sum("quantity").alias("volume"),
    F_min("price").alias("low"),
    F_max("price").alias("high"),
).show()

print("=== SILVER recorded value for the same window ===")
silver.filter(
    (col("window_start") == target_window) & (col("symbol") == "BTCUSDT")
).select("trade_count", "volume", "low", "high", "open", "close").show(truncate=False)