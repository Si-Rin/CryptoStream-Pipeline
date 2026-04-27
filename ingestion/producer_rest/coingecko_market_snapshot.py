import pandas as pd
from datetime import datetime, timezone
from ingestion.producer_rest.base_fetch import fetch_api

def get_market_snapshot(coins=None) -> pd.DataFrame:
    if coins is None:
        coins = ["bitcoin", "ethereum", "binancecoin", "ripple"]
    
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(coins),
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1
    }
    
    data = fetch_api(url, params=params)
    if not data:
        return pd.DataFrame()
    
    snapshot_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    
    rows = [{
        "snapshot_hour": snapshot_hour,
        "symbol": item["symbol"].upper(),
        "name": item["name"],
        "current_price": item["current_price"],
        "market_cap": item["market_cap"],
        "market_cap_rank": item["market_cap_rank"],
        "total_volume_24h": item["total_volume"],
        "price_change_24h_pct": item["price_change_percentage_24h"],
        "circulating_supply": item["circulating_supply"],
        "ingested_at": datetime.now(timezone.utc)
    } for item in data]
    
    return pd.DataFrame(rows)

if __name__ == "__main__":
    df = get_market_snapshot()
    print(df)