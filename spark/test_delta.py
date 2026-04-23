import sys
import os
sys.path.append(os.path.dirname(__file__))

from spark_session import get_spark_session

spark = get_spark_session()
print("✅ Spark démarré !")

data = [
    ("BTCUSDT", 84000.0, 0.5),
    ("ETHUSDT", 3200.0, 1.2),
    ("BTCUSDT", 84100.0, 0.3),
]
columns = ["symbol", "price", "qty"]

df = spark.createDataFrame(data, columns)
df.show()

# Chemin Windows au lieu de /tmp
save_path = "C:/tmp/test_delta"

df.write.format("delta").mode("overwrite").save(save_path)
print("✅ Écrit dans Delta !")

df2 = spark.read.format("delta").load(save_path)
df2.show()
print("✅ Relu depuis Delta !")
