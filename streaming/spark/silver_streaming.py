import sys
import os
sys.path.append(os.path.dirname(__file__))

from spark_session import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = get_spark_session()
print("✅ Spark démarré !")

# ── LIRE CLEAN EN STREAMING ───────────────────────────────────
print("📖 Lecture du Clean Layer en streaming...")

df = spark.readStream \
    .format("delta") \
    .load("C:/tmp/data/clean/trades")

# ── AGRÉGATION PAR MINUTE AVEC WATERMARK ─────────────────────
df_silver = df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("symbol")
    ) \
    .agg(
        first("open").alias("open"),
        max("high").alias("high"),
        min("low").alias("low"),
        last("close").alias("close"),
        sum("volume").alias("volume"),
        count("*").alias("trade_count"),
        avg("close").alias("avg_price")
    ) \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end",   col("window.end")) \
    .drop("window")

# ── ÉCRIRE DANS DELTA LAKE SILVER ────────────────────────────
print("💾 Écriture dans Delta Lake Silver...")

query = df_silver.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "C:/tmp/checkpoints/silver") \
    .start("C:/tmp/data/silver/trades")

print("✅ Silver Streaming démarré !")
query.awaitTermination()