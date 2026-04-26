from streaming.spark.spark_session import get_spark_session

spark = get_spark_session()
df = spark.read.format("delta").load("./data/bronze/trades")
df.printSchema()
print("rows:", df.count())
df.groupBy("symbol", "source").count().show()
df.orderBy("event_time", ascending=False).select(
    "symbol", "source", "price", "quantity", "event_time", "trade_id"
).show(10, truncate=False)