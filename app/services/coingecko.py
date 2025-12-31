import requests
from app.core.config import settings

BASE_URL = "https://api.coingecko.com/api/v3"

def fetch_coingecko_data(coin_id: str):
    """
    Fetches price data from CoinGecko for a specific coin_id.
    """
    url = f"{BASE_URL}/simple/price"
    params = {
        "ids": coin_id,
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true"
    }
    
    headers = {}
    if settings.COINGECKO_API_KEY:
        headers["x-cg-demo-api-key"] = settings.COINGECKO_API_KEY

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        # Return the specific coin data if it exists
        return data.get(coin_id, {})
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {coin_id} from CoinGecko: {e}")
        return None