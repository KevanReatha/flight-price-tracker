import os, time, requests
from datetime import date
from typing import Optional, Tuple

BASE_URL = "https://tequila-api.kiwi.com"

def fetch_min_price(origin: str, destination: str, dep_date: date) -> Tuple[Optional[float], Optional[int], Optional[str], dict, dict]:
    """
    Returns: (price_aud, stops, airline_code, params_used, raw_json)
    None price means: no data or request failed.
    """

    # Charger la clé API à chaque appel (pas au moment de l'import)
    api_key = os.environ.get("TEQUILA_API_KEY")
    if not api_key:
        print("[tequila] Missing TEQUILA_API_KEY")
        return None, None, None, {}, {}

    headers = {"apikey": api_key}

    params = {
        "fly_from": origin,
        "fly_to": destination,
        "date_from": dep_date.strftime("%d/%m/%Y"),
        "date_to": dep_date.strftime("%d/%m/%Y"),
        "curr": "AUD",
        "one_for_city": 1,
        "one_per_date": 1,
        "limit": 1,
        "sort": "price",
        "adults": 1,
    }

    # simple retry (handles transient 429/5xx)
    for attempt in range(3):
        try:
            r = requests.get(f"{BASE_URL}/v2/search", headers=headers, params=params, timeout=25)
            if r.status_code == 200:
                data = r.json()
                if not data or not data.get("data"):
                    return None, None, None, params, data or {}
                first = data["data"][0]
                price = float(first.get("price"))
                stops = max(0, len(first.get("route", [])) - 1)
                airline = first.get("airlines", [None])[0]
                return price, stops, airline, params, data
            else:
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(1 + attempt)  # backoff
                    continue
                return None, None, None, params, {}
        except requests.RequestException:
            time.sleep(1 + attempt)

    return None, None, None, params, {}