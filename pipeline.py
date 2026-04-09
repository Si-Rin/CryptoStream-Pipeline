"""
This module imports the necessary functions from the ingestion, transformation, and storage modules,and defines a main function to run the entire pipeline.
"""
from ingestion.binance import get_binance_data
from ingestion.coingecko import get_coingecko_data
from ingestion.kraken import get_kraken_data
from transformation.transform import clean_data
from storage.save import save_parquet, save_raw

def run_pipeline():
    """
    Orchestrate the data flow the data flow from ingestion to transformation and storage.
    
    The function performs the following steps:
        1. Fetches data from Binance, Coingecko and Kraken APIs using the respective functions.    
        2. Cleans and transforms the fetched data using the clean_data function.
        3. Saves the cleaned data to a Parquet file using the save_parquet function.
    """
    
    # Fetch data from the APIs
    print("\n"+"-"*50)
    print("Starting data collection...")
    print("-"*50+"\n")
    
    print("Collecting data from Binance API...")
    binance = get_binance_data()
    print("Collecting data from CoinGecko API...")
    coingecko = get_coingecko_data()
    print("Collecting data from Kraken API...")
    kraken = get_kraken_data()
    print("Data collection completed.")
    
    # Save the raw data to JSON files for reference
    print("\n"+"-"*50)
    print("Saving raw data to JSON files...")
    print("-"*50+"\n")
    
    save_raw(binance, "raw_data/binance_raw.json")
    save_raw(coingecko, "raw_data/coingecko_raw.json")
    save_raw(kraken, "raw_data/kraken_raw.json")
    print("Raw data saved to JSON files.")

if __name__ == "__main__":
    run_pipeline()