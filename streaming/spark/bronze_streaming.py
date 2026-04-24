import sys
import os
sys.path.append(os.path.dirname(__file__))

from spark_session import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = get_spark_session()
print("✅ Spark démarré !")

# Schéma des messages Kafka
schema = StructType([
    StructField("source",    StringType(),  True),
    StructField("symbol",    StringType(),  True),
    StructField("timestamp", LongType(),    True),
    StructField("open",      DoubleType(),  True),
    StructField("high",      DoubleType(),  True),
    StructField("low",       DoubleType(),  True),
    StructField("close",     DoubleType(),  True),
    StructField("volume",    DoubleType(),  True),
])

# ── LIRE DEPUIS KAFKA ─────────────────────────────────────────
print("📖 Lecture depuis Kafka...")

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "crypto-trades-btcusdt,crypto-trades-ethusdt,crypto-trades-bnbusdt,crypto-trades-xrpusdt") \
    .option("startingOffsets", "latest") \
    .load()

# Décoder les messages JSON depuis Kafka
df = df_kafka.select(
    from_json(
        col("value").cast("string"),
        schema
    ).alias("data")
).select("data.*")

# Ajouter timestamp d'ingestion
df = df.withColumn("ingested_at", current_timestamp())

# ── ÉCRIRE DANS DELTA LAKE BRONZE ────────────────────────────
print("💾 Écriture dans Delta Lake Bronze...")

query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "C:/tmp/checkpoints/bronze") \
    .start("C:/tmp/data/bronze/trades")

print("✅ Bronze Streaming démarré !")
query.awaitTermination()