import sys
import os
sys.path.append(os.path.dirname(__file__))

from spark_session import get_spark_session
from pyspark.sql.functions import current_timestamp, to_timestamp, col
from pyspark.sql.types import *

spark = get_spark_session()
print("✅ Spark démarré !")

# Lire les fichiers JSON avec multiLine=True
# car tes fichiers sont des tableaux [ {...}, {...} ]
print("📖 Lecture des fichiers JSON...")
df = spark.read \
    .option("multiLine", "true") \
    .json("raw_data/*.json")

# Supprimer les lignes nulles
df = df.filter(col("symbol").isNotNull())

# Convertir timestamp en datetime lisible
df = df.withColumn(
    "event_time",
    to_timestamp(col("timestamp") / 1000)
)

# Ajouter colonne ingestion time
df = df.withColumn("ingested_at", current_timestamp())

# Afficher un aperçu
print(f"✅ Nombre de lignes lues : {df.count()}")
df.show(5)

# Écrire dans Delta Lake Bronze
print("💾 Écriture dans Delta Lake Bronze...")
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("C:/tmp/data/bronze/trades")

print("✅ Bronze Layer créé !")

# Vérification
df_check = spark.read.format("delta").load("C:/tmp/data/bronze/trades")
print(f"✅ Lignes dans Bronze : {df_check.count()}")
df_check.show(5)