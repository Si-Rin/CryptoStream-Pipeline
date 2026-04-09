import pandas as pd
from ingestion.base_fetch import fetch_api

def get_binance_data(symbols=None, limit=300) -> pd.DataFrame:
    """ 
    Fetches OHLCV (Open, High, Low, Close, Volume) data from Binance API for the specified symbols and returns it as a DataFrame.
    
    Parameters:
        - symbols: List of trading pairs to fetch data for (e.g., ["BTCUSDT", "ETHUSDT"]). If None, a default list of popular trading pairs will be used.
        - limit: Number of data points to fetch for each symbol (default is 300).       
    
    Returns:
        A pandas DataFrame containing the OHLCV data for the specified symbols, with columns for source, symbol, timestamp, open, high, low, close, and volume. 
    
    The function performs the following steps:
        1. Defines the Binance API endpoint for klines (candlestick data).
        2. Initializes an empty list to hold the fetched data and if no symbols are provided, it uses a default list of popular trading pairs.
        3. For each symbol, fetches the data using the base fetch function.
        4. It checks if the data is valid and in list format. If not, it skips to the next symbol.
        5. For each valid data point (candle), appends a dictionary containing the source, symbol, timestamp, open, high, low, close, and volume to all_data list.
        6. Converts the list of dictionaries to a pandas DataFrame and returns it.
    """
    
    # Binance API endpoint for klines (candlestick data)
    url = "https://data-api.binance.vision/api/v3/klines"
    
    # If no symbols are provided, use a default list of popular trading pairs
    if symbols is None:
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

    # List to hold all the fetched data
    all_data = []

    # Fetch data for each symbol
    for symbol in symbols:
        # Set the parameters for the API request
        params = {
            "symbol": symbol,
            "interval": "1m",
            "limit": 300
        }
        
        print(f"Collecting Binance : {symbol} ...")

        # Fetch the data using the base fetch function
        data = fetch_api(url, params=params)

        # Check if the data is valid and in list format
        if not isinstance(data, list):
            continue

        # Append each candlestick data point to all_data list
        for candle in data:
            all_data.append({
                "source": "binance",
                "symbol": symbol,
                "timestamp": candle[0],
                "open": float(candle[1]),
                "high": float(candle[2]),
                "low": float(candle[3]),
                "close": float(candle[4]),
                "volume": float(candle[5])
            })

    print("Binance data collection completed.\n")
    
    # Convert the list of dictionaries to a DataFrame and return it
    return pd.DataFrame(all_data)