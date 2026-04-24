import sys
import os
sys.path.append(os.path.dirname(__file__))

from spark_session import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = get_spark_session()
print("✅ Spark démarré !")

# ── LIRE SILVER EN BATCH ──────────────────────────────────────
print("📖 Lecture du Silver Layer...")

df = spark.read.format("delta").load("C:/tmp/data/silver/trades")

# Fenêtres glissantes
w14  = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-13, 0)
w26  = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-25, 0)
w12  = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-11, 0)
w20  = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-19, 0)
w9   = Window.partitionBy("symbol").orderBy("window_start").rowsBetween(-8,  0)
w_lag = Window.partitionBy("symbol").orderBy("window_start")

# ── RSI ───────────────────────────────────────────────────────
print("⚙️ Calcul RSI...")
df = df.withColumn("prev_close", lag("close", 1).over(w_lag))
df = df.withColumn("change", col("close") - col("prev_close"))
df = df.withColumn("gain",
    when(col("change") > 0, col("change")).otherwise(0))
df = df.withColumn("loss",
    when(col("change") < 0, -col("change")).otherwise(0))
df = df.withColumn("avg_gain", avg("gain").over(w14))
df = df.withColumn("avg_loss", avg("loss").over(w14))
df = df.withColumn("rsi",
    when(col("avg_loss") == 0, lit(100.0))
    .otherwise(100 - (100 / (1 + col("avg_gain") / col("avg_loss"))))
)
print("✅ RSI calculé !")

# ── MACD ──────────────────────────────────────────────────────
print("⚙️ Calcul MACD...")
df = df.withColumn("ema12", avg("close").over(w12))
df = df.withColumn("ema26", avg("close").over(w26))
df = df.withColumn("macd", col("ema12") - col("ema26"))
df = df.withColumn("signal_line", avg("macd").over(w9))
df = df.withColumn("histogram", col("macd") - col("signal_line"))
print("✅ MACD calculé !")

# ── BOLLINGER BANDS ───────────────────────────────────────────
print("⚙️ Calcul Bollinger Bands...")
df = df.withColumn("bb_middle", avg("close").over(w20))
df = df.withColumn("bb_std", stddev("close").over(w20))
df = df.withColumn("bb_upper", col("bb_middle") + 2 * col("bb_std"))
df = df.withColumn("bb_lower", col("bb_middle") - 2 * col("bb_std"))
print("✅ Bollinger Bands calculé !")

# ── COLONNES FINALES ──────────────────────────────────────────
df_gold = df.select(
    "symbol", "window_start", "window_end",
    "open", "high", "low", "close",
    "volume", "trade_count", "avg_price",
    "rsi", "macd", "signal_line", "histogram",
    "bb_upper", "bb_middle", "bb_lower"
)

# ── ÉCRIRE DANS DELTA LAKE GOLD ──────────────────────────────
print("💾 Écriture dans Delta Lake Gold...")
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save("C:/tmp/data/gold/trades")

print("✅ Gold Layer créé !")