from streaming.spark.spark_session import get_spark_session
from pyspark.sql.functions import col

spark = get_spark_session()

gold = spark.read.format("delta").load("./data/gold/indicators_1m")

# 1. Schema check
gold.printSchema()

# 2. Total rows by symbol — should match Silver row counts
gold.groupBy("symbol").count().orderBy("symbol").show()

# 3. NULL pattern check — early windows should have NULLs, mature ones shouldn't
gold.filter("symbol = 'BTCUSDT'").select(
    "window_start", "candle_idx" if False else "close",  # candle_idx not in projection; ignore
    "rsi", "macd_approx", "signal_approx", "bb_middle"
).orderBy("window_start").show(40, truncate=False)

# 4. Indicator sanity for the most recent BTCUSDT row
gold.filter(col("symbol")=="BTCUSDT").orderBy(col("window_start").desc()).select(
    "window_start", "close", "rsi",
    "ma12", "ma26", "macd_approx", "signal_approx", "macd_histogram",
    "bb_lower", "bb_middle", "bb_upper"
).show(5, truncate=False)