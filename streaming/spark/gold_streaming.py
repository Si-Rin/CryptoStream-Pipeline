"""
Gold layer — technical indicators (RSI, MACD-approx, Bollinger Bands).

Reads the entire Silver OHLCV table in batch, computes window-based
indicators per symbol, overwrites Gold.

Run on a schedule (every 60s) by pipeline_streaming.py.

Indicator notes:
- RSI uses simple moving averages of gains/losses (not Wilder smoothing).
  This is a documented simplification; see report.
- MACD uses simple moving averages, not true EMAs. The crossover semantics
  are preserved but magnitudes differ from textbook EMA-based MACD.
  Columns are named honestly: ma12, ma26, macd_approx, signal_approx.
- Bollinger uses population stddev (stddev_pop) over a 20-window.
- Indicators are NULL until enough candles exist (14 for RSI, 26 for MACD,
  20 for Bollinger).
"""
import os
import sys
sys.path.append(os.path.dirname(__file__))

from dotenv import load_dotenv
from spark_session import get_spark_session
from pyspark.sql import functions as F
from pyspark.sql.window import Window

load_dotenv()

SILVER_PATH = os.getenv("SILVER_PATH", "./data/silver/ohlcv_1m")
GOLD_PATH   = os.getenv("GOLD_PATH",   "./data/gold/indicators_1m")

spark = get_spark_session()
spark.sparkContext.setLogLevel("WARN")
print(f"Gold: silver={SILVER_PATH} | gold={GOLD_PATH}")

silver = spark.read.format("delta").load(SILVER_PATH)

# Common ordering: by window_start within each symbol
w_lag = Window.partitionBy("symbol").orderBy("window_start")
w14   = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-13, 0)
w12   = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-11, 0)
w26   = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-25, 0)
w20   = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-19, 0)
w9    = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-8, 0)

# Count of candles seen so far per symbol — used to gate immature indicators
w_count = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(
    Window.unboundedPreceding, 0
)

df = silver.withColumn("candle_idx", F.count("close").over(w_count))

# ── RSI ─────────────────────────────────────────────────────
df = df.withColumn("prev_close", F.lag("close", 1).over(w_lag))
df = df.withColumn("change",     F.col("close") - F.col("prev_close"))
df = df.withColumn("gain", F.when(F.col("change") > 0, F.col("change")).otherwise(0.0))
df = df.withColumn("loss", F.when(F.col("change") < 0, -F.col("change")).otherwise(0.0))
df = df.withColumn("avg_gain_14", F.avg("gain").over(w14))
df = df.withColumn("avg_loss_14", F.avg("loss").over(w14))
df = df.withColumn(
    "rsi",
    F.when(
        F.col("candle_idx") >= 14,
        F.when(F.col("avg_loss_14") == 0, F.lit(100.0))
         .otherwise(100 - (100 / (1 + F.col("avg_gain_14") / F.col("avg_loss_14"))))
    )
)

# ── MACD (SMA approximation) ────────────────────────────────
df = df.withColumn("ma12", F.avg("close").over(w12))
df = df.withColumn("ma26", F.avg("close").over(w26))
df = df.withColumn(
    "macd_approx",
    F.when(F.col("candle_idx") >= 26, F.col("ma12") - F.col("ma26"))
)
df = df.withColumn(
    "signal_approx",
    F.when(F.col("candle_idx") >= 26 + 9, F.avg("macd_approx").over(w9))
)
df = df.withColumn(
    "macd_histogram",
    F.col("macd_approx") - F.col("signal_approx")
)

# ── Bollinger Bands ─────────────────────────────────────────
df = df.withColumn(
    "bb_middle",
    F.when(F.col("candle_idx") >= 20, F.avg("close").over(w20))
)
df = df.withColumn("bb_std", F.stddev_pop("close").over(w20))
df = df.withColumn(
    "bb_upper",
    F.when(F.col("candle_idx") >= 20, F.col("bb_middle") + 2 * F.col("bb_std"))
)
df = df.withColumn(
    "bb_lower",
    F.when(F.col("candle_idx") >= 20, F.col("bb_middle") - 2 * F.col("bb_std"))
)

# Final projection
gold = df.select(
    "symbol", "window_start", "window_end",
    "open", "high", "low", "close",
    "volume", "trade_count", "avg_price", "vwap",
    "rsi",
    "ma12", "ma26", "macd_approx", "signal_approx", "macd_histogram",
    "bb_middle", "bb_upper", "bb_lower",
)

print(f"Writing Gold: input row count = {silver.count()}")
gold.write.format("delta").mode("overwrite").save(GOLD_PATH)
print("Gold updated.")