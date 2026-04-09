"""
Base fetch module for API data retrieval.
Provides a common function to fetch data from APIs with retry logic.
"""
import requests
import time

def fetch_api(url, params=None, retries=3): # 3 retries by default
    """
    Fetch data from a REST API endpoint with retry logic.

    Args:
        url (str): The API endpoint URL.
        params (dict, optional): The query parameters for the API request. Defaults to None.
        retries (int, optional): The number of retry attempts. Defaults to 3.

    Returns:
        dict: The JSON response from the API or an error message.
        
    The function performs the following steps:
        1. Attempts to make a GET request to the specified URL with the provided parameters.
        2. If the request is successful (status code 200), returns the JSON response.
        3. If an exception occurs during the request, catches the exception and prints an error message.
        4. If the request fails, waits for a short period before retrying, up to the specified number of retries.
        5. If all retry attempts fail, returns None.
    """
    for attempt in range(retries):
        try:
            # Make the API request
            response = requests.get(url, params=params)

            if response.status_code == 200:
                return response.json()

        except Exception as e:
            print(f"Error: {e}")

        # Wait before retrying        
        print(f"Retrying... (Attempt {attempt + 1}/{retries})")
        time.sleep(1)

    return None