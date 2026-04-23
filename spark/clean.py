import sys
import os
sys.path.append(os.path.dirname(__file__))

from spark_session import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = get_spark_session()
print("✅ Spark démarré !")

# Lire le Bronze Layer
print("📖 Lecture du Bronze Layer...")
df = spark.read.format("delta").load("C:/tmp/data/bronze/trades")
print(f"Lignes dans Bronze : {df.count()}")

# ── 1. CORRIGER LE TIMESTAMP ──────────────────────────────────
# Binance/CoinGecko → millisecondes → diviser par 1000
# Kraken            → secondes      → garder tel quel
df = df.withColumn(
    "event_time",
    when(
        col("timestamp") > 9999999999,
        to_timestamp(col("timestamp") / 1000)
    ).otherwise(
        to_timestamp(col("timestamp"))
    )
)
print("✅ Timestamp corrigé !")

# ── 2. NORMALISER LES SYMBOLES ────────────────────────────────
# Tout ramener au format standard XXXUSDT
df = df.withColumn(
    "symbol",
    # Kraken
    when(col("symbol") == "XBTUSD",      lit("BTCUSDT"))
    .when(col("symbol") == "ETHUSD",      lit("ETHUSDT"))
    .when(col("symbol") == "BNBUSD",      lit("BNBUSDT"))
    .when(col("symbol") == "XETHUSDT",    lit("ETHUSDT"))
    .when(col("symbol") == "XRPUSD",      lit("XRPUSDT"))  # Kraken XRP
    # CoinGecko
    .when(col("symbol") == "BITCOIN",     lit("BTCUSDT"))
    .when(col("symbol") == "ETHEREUM",    lit("ETHUSDT"))
    .when(col("symbol") == "BINANCECOIN", lit("BNBUSDT"))
    .when(col("symbol") == "RIPPLE",      lit("XRPUSDT"))  # CoinGecko XRP
    # Binance déjà bon (XRPUSDT)
    .otherwise(col("symbol"))
)
print("✅ Symboles normalisés !")

# ── 3. CORRIGER COINGECKO (pas de open/high/low) ─────────────
# CoinGecko n'a que close et volume
# On met open/high/low = close pour ne pas perdre ces données
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
print("✅ Valeurs nulles corrigées !")

# ── 4. SUPPRIMER LES LIGNES INUTILES ─────────────────────────
df_clean = df.filter(
    col("symbol").isNotNull() &
    col("close").isNotNull() &
    col("volume").isNotNull() &
    col("event_time").isNotNull()
)

# ── 5. GARDER SEULEMENT LES COLONNES UTILES ──────────────────
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

# Aperçu par source
print("\n📊 Aperçu des données nettoyées :")
df_clean.groupBy("source", "symbol").count().show()

print(f"\n✅ Lignes après nettoyage : {df_clean.count()}")
df_clean.show(5)

# ── 6. ÉCRIRE DANS DELTA LAKE CLEAN ──────────────────────────
print("💾 Écriture dans Delta Lake Clean...")
df_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save("C:/tmp/data/clean/trades")

print("✅ Clean Layer créé !")

# Vérification
df_check = spark.read.format("delta").load("C:/tmp/data/clean/trades")
print(f"✅ Lignes dans Clean : {df_check.count()}")
df_check.show(5)