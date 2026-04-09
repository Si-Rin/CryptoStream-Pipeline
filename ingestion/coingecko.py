import pandas as pd
from ingestion.base_fetch import fetch_api

def get_coingecko_data(coins=None) -> pd.DataFrame:
    """
    Fetches market data for specified coins from the CoinGecko API and returns it as a DataFrame.
    
    Parameters:
        - coins: List of coin IDs to fetch data for (e.g., ["bitcoin", "ethereum"]). If None, a default list of popular coins will be used.
    
    Returns:
        A pandas DataFrame containing the market data for the specified coins, with columns for source, symbol, timestamp, close price, and volume.
        
    The function performs the following steps:
        1. Defines the CoinGecko API endpoint for market chart data.
        2. Initializes an empty list to hold the fetched data and if no coins are provided, it uses a default list of popular coins.
        3. For each coin, fetches the data using the base fetch function.
        4. Checks if the data is valid and contains the expected key ("prices"). If not, it prints an error message and skips to the next coin.
        5. For each valid data point (price and volume), appends a dictionary containing the source, symbol, timestamp, close price, and volume to the all_data list.
        6. Converts the list of dictionaries to a pandas DataFrame and returns it.
    """
    
    # List of coins to fetch data for (if no coins provided, use a default list)
    if coins is None:
        coins = ["bitcoin", "ethereum", "binancecoin"]

   # List to hold all the fetched data
    all_data = []

    # Fetch data for each coin
    for coin in coins:
        url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart"

        params = {
            "vs_currency": "usd",
            "days": 1
        }

        print(f"Collecting CoinGecko : {coin} ...")
        
        # Fetch the data using the base fetch function
        data = fetch_api(url, params=params)

        # Check if the data is valid and contains the expected key ("prices")
        if not data or "prices" not in data:
            print(f"❌ Erreur API pour {coin} :", data)
            continue

        # Extract the relevant data from the API response
        prices = data["prices"]
        volumes = data["total_volumes"]

        # Append each price and volume data point to all_data list
        for i in range(len(prices)):
            all_data.append({
                "source": "coingecko",
                "symbol": coin.upper(),
                "timestamp": prices[i][0],
                "close": prices[i][1],
                "volume": volumes[i][1]
            })
            
    print("CoinGecko data collection completed.\n")

    # Convert the list of dictionaries to a DataFrame and return it
    return pd.DataFrame(all_data)