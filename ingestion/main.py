import os, json
from datetime import date, datetime, timedelta, timezone
from typing import List, Tuple
from dotenv import load_dotenv
from utils.snowflake_io import insert_quotes, insert_raw_json
from providers.tequila import fetch_min_price

load_dotenv()  # Why: keep secrets/config out of code (12-factor)

ROUTES: List[Tuple[str, str]] = [("MEL","SYD"), ("MEL","HKG"), ("MEL","SIN")]
HORIZON_DAYS = int(os.environ.get("HORIZON_DAYS", "30"))  # Why: control API cost/limits
STORE_JSON   = os.environ.get("STORE_JSON", "0") == "1"   # off by default
SOURCE_NAME = os.environ.get("SOURCE_NAME", "tequila")

def run_once():
    now = datetime.now(timezone.utc)
    batch = []
    for origin, dest in ROUTES:
        for i in range(1, HORIZON_DAYS + 1):
            dep = date.today() + timedelta(days=i)
            price, stops, airline, params, raw = fetch_min_price(origin, dest, dep)
            if price is None:
                # TEMP DEBUG:
                # print(f"[skip] {origin}-{dest} {dep}: no price")
                continue
            batch.append((origin, dest, dep, now, price, int(stops) if stops is not None else None, airline, SOURCE_NAME, "Y"))
            if STORE_JSON and raw:
                insert_raw_json(f"{origin}-{dest}", json.dumps(params), json.dumps(raw), now)

    n = insert_quotes(batch)
    print(f"Ingestion complete: inserted {n} rows.")

if __name__ == "__main__":
    run_once()