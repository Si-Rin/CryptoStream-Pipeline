import pandas as pd
from ingestion.base_fetch import fetch_api

def get_kraken_data(symbols=None) -> pd.DataFrame:
    """ 
    Fetches OHLCV (Open, High, Low, Close, Volume) data from Kraken API for the specified symbols and returns it as a DataFrame.
    
    Parameters:
        - symbols: List of trading pairs to fetch data for (e.g., ["XBTUSD", "ETHUSD"]). If None, a default list of popular trading pairs will be used.
        
    Returns:
        A pandas DataFrame containing the OHLCV data for the specified symbols, with columns for source, symbol, timestamp, open, high, low, close, and volume. 
    
    The function performs the following steps:
        1. Defines the Kraken API endpoint for OHLC data.
        2. Initializes an empty list to hold the fetched data and if no symbols are provided, it uses a default list of popular trading pairs.
        3. For each symbol, fetches the data using the base fetch function.
        4. It checks if the data is valid and does not contain errors. If not, it skips to the next symbol.
        5. For each valid data point (candle), appends a dictionary containing the source, symbol, timestamp, open, high, low, close, and volume to all_data list.
        6. Converts the list of dictionaries to a pandas DataFrame and returns it.
    """
    
    # Kraken API endpoint for OHLC data
    url = "https://api.kraken.com/0/public/OHLC"
    
    # If no symbols are provided, use a default list of popular trading pairs
    if symbols is None:
        symbols = ["XBTUSD", "ETHUSD", "BNBUSD", "XRPUSD"]

    # List to hold all the fetched data
    all_data = []

    # Fetch data for each symbol
    for symbol in symbols:
        # Set the parameters for the API request
        params = {
            "pair": symbol,
            "interval": 1
        }

        print(f"Collecting Kraken : {symbol} ...")
        
        # Fetch the data using the base fetch function
        data = fetch_api(url, params=params)

        # Check if the data is valid and does not contain errors
        if not data or data["error"]:
            continue
        
        # Extract the relevant data from the API response
        result = data["result"]
        # The API returns a dictionary where the key is the trading pair, so we need to get that key to access the candles
        pair_key = list(result.keys())[0]
        # Extract the candlestick data for the trading pair
        candles = result[pair_key]

        # Append each candlestick data point to all_data list
        for candle in candles:
            all_data.append({
                "source": "kraken",
                "symbol": symbol,
                "timestamp": candle[0],
                "open": float(candle[1]),
                "high": float(candle[2]),
                "low": float(candle[3]),
                "close": float(candle[4]),
                # The API returns volume in the 7th position (index 6) of the candle data, not the 6th position (index 5) like Binance
                "volume": float(candle[6])
            })
            
    print("Kraken data collection completed.\n")

    # Convert the list of dictionaries to a DataFrame and return it
    return pd.DataFrame(all_data)