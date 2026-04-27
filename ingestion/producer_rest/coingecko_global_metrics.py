import pandas as pd
from datetime import datetime, timezone
from ingestion.producer_rest.base_fetch import fetch_api

def get_global_metrics() -> pd.DataFrame:
    url = "https://api.coingecko.com/api/v3/global"
    data = fetch_api(url)
    if not data or "data" not in data:
        return pd.DataFrame()
    
    d = data["data"]
    snapshot_hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    
    return pd.DataFrame([{
        "snapshot_hour": snapshot_hour,
        "total_market_cap_usd": d["total_market_cap"]["usd"],
        "total_volume_24h_usd": d["total_volume"]["usd"],
        "btc_dominance": d["market_cap_percentage"]["btc"],
        "eth_dominance": d["market_cap_percentage"]["eth"],
        "active_cryptocurrencies": d["active_cryptocurrencies"],
        "markets": d["markets"],
        "ingested_at": datetime.now(timezone.utc)
    }])
    
if __name__ == "__main__":
    df = get_global_metrics()
    print(df)