import requests
import time
import logging

logger = logging.getLogger(__name__)

def fetch_api(url, params=None, retries=3, backoff_base=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                return response.json()
            if response.status_code == 429:  # rate limit
                wait = backoff_base ** (attempt + 2)  # 4s, 8s, 16s
                logger.warning(f"Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            logger.error(f"HTTP {response.status_code}: {response.text[:200]}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
        time.sleep(backoff_base ** attempt)  # 1s, 2s, 4s
    return None