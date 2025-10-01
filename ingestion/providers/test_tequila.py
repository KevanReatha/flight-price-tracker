import os
import requests
from dotenv import load_dotenv

load_dotenv()  # charge ton .env

API_KEY = os.environ.get("TEQUILA_API_KEY")
if not API_KEY:
    raise RuntimeError("TEQUILA_API_KEY missing from .env")

url = "https://tequila-api.kiwi.com/v2/search"
params = {
    "fly_from": "MEL",
    "fly_to": "SYD",
    "date_from": "15/10/2025",
    "date_to": "15/10/2025",
    "curr": "AUD",
    "limit": 1,
}
headers = {"apikey": API_KEY}

r = requests.get(url, headers=headers, params=params, timeout=20)
print("Status:", r.status_code)
print("JSON Response:")
print(r.json())
