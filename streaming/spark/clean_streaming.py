import sys
import os
sys.path.append(os.path.dirname(__file__))

from spark_session import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = get_spark_session()
print("✅ Spark démarré !")

# ── LIRE BRONZE EN STREAMING ──────────────────────────────────
print("📖 Lecture du Bronze Layer en streaming...")

df = spark.readStream \
    .format("delta") \
    .load("C:/tmp/data/bronze/trades")

# ── 1. CORRIGER LE TIMESTAMP ──────────────────────────────────
df = df.withColumn(
    "event_time",
    when(
        col("timestamp") > 9999999999,
        to_timestamp(col("timestamp") / 1000)
    ).otherwise(
        to_timestamp(col("timestamp"))
    )
)

# ── 2. NORMALISER LES SYMBOLES ────────────────────────────────
df = df.withColumn(
    "symbol",
    # Kraken
    when(col("symbol") == "XBTUSD",      lit("BTCUSDT"))
    .when(col("symbol") == "ETHUSD",      lit("ETHUSDT"))
    .when(col("symbol") == "BNBUSD",      lit("BNBUSDT"))
    .when(col("symbol") == "XRPUSD",      lit("XRPUSDT"))
    # CoinGecko
    .when(col("symbol") == "BITCOIN",     lit("BTCUSDT"))
    .when(col("symbol") == "ETHEREUM",    lit("ETHUSDT"))
    .when(col("symbol") == "BINANCECOIN", lit("BNBUSDT"))
    .when(col("symbol") == "RIPPLE",      lit("XRPUSDT"))
    # Binance déjà bon
    .otherwise(col("symbol"))
)

# ── 3. CORRIGER COINGECKO ─────────────────────────────────────
df = df.withColumn(
    "open",
    when(col("open").isNull(), col("close")).otherwise(col("open"))
)
df = df.withColumn(
    "high",
    when(col("high").isNull(), col("close")).otherwise(col("high"))
)
df = df.withColumn(
    "low",
    when(col("low").isNull(), col("close")).otherwise(col("low"))
)

# ── 4. SUPPRIMER LIGNES INUTILES ──────────────────────────────
df_clean = df.filter(
    col("symbol").isNotNull() &
    col("close").isNotNull() &
    col("volume").isNotNull() &
    col("event_time").isNotNull()
)

# ── 5. GARDER COLONNES UTILES ─────────────────────────────────
df_clean = df_clean.select(
    "source",
    "symbol",
    "event_time",
    "open",
    "high",
    "low",
    "close",
    "volume"
)

# ── 6. ÉCRIRE DANS DELTA LAKE CLEAN ──────────────────────────
print("💾 Écriture dans Delta Lake Clean...")

query = df_clean.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "C:/tmp/checkpoints/clean") \
    .start("C:/tmp/data/clean/trades")

print("✅ Clean Streaming démarré !")
query.awaitTermination()