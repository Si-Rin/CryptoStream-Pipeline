from deltalake import DeltaTable
import pandas as pd

df = DeltaTable("./data/gold/indicators_1m").to_pandas()
print(df[["symbol","window_start","close","avg_price","vwap","rsi","macd_approx","signal_approx"]].tail(10))