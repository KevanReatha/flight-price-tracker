import os, requests
from datetime import date
from typing import Optional, Tuple

BASE_URL = "https://tequila-api.kiwi.com"          # example provider
API_KEY = os.environ.get("TEQUILA_API_KEY")
HEADERS = {"apikey": API_KEY} if API_KEY else {}

def fetch_min_price(origin: str, destination: str, dep_date: date) -> Tuple[Optional[float], Optional[int], Optional[str], dict, dict]:
    """
    Returns: (price_aud, stops, airline_code, params_used, raw_json)
    """
    if not API_KEY:
        return None, None, None, {}, {}  # no key → skip

    params = {
        "fly_from": origin,
        "fly_to": destination,
        "date_from": dep_date.strftime("%d/%m/%Y"),
        "date_to": dep_date.strftime("%d/%m/%Y"),
        "curr": "AUD",
        "one_for_city": 1,
        "limit": 1,
        "sort": "price",
    }
    try:
        r = requests.get(f"{BASE_URL}/v2/search", headers=HEADERS, params=params, timeout=25)
    except requests.RequestException:
        return None, None, None, params, {}
    if r.status_code != 200:
        return None, None, None, params, {}
    data = r.json()
    if not data.get("data"):
        return None, None, None, params, data

    first = data["data"][0]
    price   = float(first.get("price"))
    stops   = max(0, len(first.get("route", [])) - 1)
    airline = first.get("airlines", [None])[0]
    return price, stops, airline, params, data