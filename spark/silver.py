import sys
import os
sys.path.append(os.path.dirname(__file__))

from spark_session import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = get_spark_session()
print("✅ Spark démarré !")

# Lire le Clean Layer
print("📖 Lecture du Clean Layer...")
df = spark.read.format("delta").load("C:/tmp/data/clean/trades")
print(f"Lignes dans Clean : {df.count()}")

# ── AGRÉGATION PAR MINUTE ET PAR SYMBOLE ─────────────────────
# Pour chaque minute et chaque symbole on calcule :
# open   = premier prix de la minute
# high   = prix le plus haut
# low    = prix le plus bas
# close  = dernier prix de la minute
# volume = volume total
# trade_count = nombre de trades

print("⚙️ Calcul des agrégations par minute...")

# Tronquer event_time à la minute
df = df.withColumn(
    "minute",
    date_trunc("minute", col("event_time"))
)

# Fenêtre pour avoir le premier et dernier prix
from pyspark.sql.window import Window

w_first = Window.partitionBy("symbol", "minute").orderBy("event_time")
w_last  = Window.partitionBy("symbol", "minute").orderBy(col("event_time").desc())

df = df.withColumn("first_price", first("open").over(w_first))
df = df.withColumn("last_price",  first("close").over(w_last))

# Agrégation principale
df_silver = df.groupBy("symbol", "minute").agg(
    first("first_price").alias("open"),
    max("high").alias("high"),
    min("low").alias("low"),
    first("last_price").alias("close"),
    sum("volume").alias("volume"),
    count("*").alias("trade_count"),
    avg("close").alias("avg_price")
)

# Renommer minute en window_start
df_silver = df_silver.withColumnRenamed("minute", "window_start")

# Trier par symbole et temps
df_silver = df_silver.orderBy("symbol", "window_start")

# Aperçu
print("\n📊 Aperçu Silver Layer :")
df_silver.groupBy("symbol").count().show()

print(f"\n✅ Lignes Silver : {df_silver.count()}")
df_silver.show(10)

# Écrire dans Delta Lake Silver
print("💾 Écriture dans Delta Lake Silver...")
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save("C:/tmp/data/silver/trades")

print("✅ Silver Layer créé !")

# Vérification
df_check = spark.read.format("delta").load("C:/tmp/data/silver/trades")
print(f"✅ Lignes dans Silver : {df_check.count()}")
df_check.show(10)