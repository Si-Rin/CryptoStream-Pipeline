from streaming.spark.spark_session import get_spark_session
from pyspark.sql.functions import col, min as F_min, max as F_max

spark = get_spark_session()
silver = spark.read.format("delta").load("./data/silver/ohlcv_1m")
print("Silver total rows:", silver.count())
silver.agg(F_min("window_start"), F_max("window_start")).show(truncate=False)
silver.groupBy("symbol").count().orderBy("symbol").show()
silver.filter(col("symbol")=="BTCUSDT").orderBy(col("window_start").desc()).show(5, truncate=False)