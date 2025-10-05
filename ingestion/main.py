import os, json
from datetime import date, datetime, timedelta, timezone
from typing import List, Tuple
from dotenv import load_dotenv
import snowflake.connector  # <-- NEW

from ingestion.providers.tequila import fetch_min_price
from ingestion.utils.snowflake_io import insert_quotes, insert_raw_json

load_dotenv()  # keep secrets/config out of code

# ----------------------- Defaults / fallbacks -----------------------
ORIGIN = os.environ.get("ORIGIN", "MEL").upper()
DEFAULT_DESTS = "BKK,PNH,SGN,MNL,HND,ICN"
DESTINATIONS = os.environ.get("DESTINATIONS", DEFAULT_DESTS)
DEST_LIST: List[str] = [d.strip().upper() for d in DESTINATIONS.split(",") if d.strip()]

ROUTES_CSV = os.environ.get("ROUTES_CSV", "").strip()

HORIZON_DAYS = int(os.environ.get("HORIZON_DAYS", "60"))
STORE_JSON   = os.environ.get("STORE_JSON", "0") == "1"
SOURCE_NAME  = os.environ.get("SOURCE_NAME", "tequila")

# ----------------------- Snowflake connection (for reading routes) -----------------------
def _sf_connect():
    """Tiny helper: connect using the same .env vars you already set."""
    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )
    # Key-pair auth (recommended)
    pk_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if pk_path:
        kwargs["private_key_file"] = pk_path
    else:
        # Fallback to password if you set SNOWFLAKE_PASSWORD
        pwd = os.environ.get("SNOWFLAKE_PASSWORD")
        if not pwd:
            raise RuntimeError("Snowflake auth not configured: set PRIVATE_KEY_PATH or PASSWORD")
        kwargs["password"] = pwd
    return snowflake.connector.connect(**kwargs)

def fetch_supported_routes_from_sf() -> List[Tuple[str, str]]:
    """
    Read supported routes from CORE.DIM_SUPPORTED_ROUTES (values like 'MEL-BKK').
    Returns list of tuples: [('MEL','BKK'), ...]
    """
    sql = "SELECT route_code FROM FLIGHT_DB.CORE.DIM_SUPPORTED_ROUTES"
    with _sf_connect() as con:
        cur = con.cursor()
        try:
            cur.execute(sql)
            rows = [r[0] for r in cur.fetchall()]
        finally:
            cur.close()
    pairs: List[Tuple[str, str]] = []
    for rc in rows:
        if not rc or "-" not in rc:
            continue
        o, d = rc.strip().upper().split("-", 1)
        pairs.append((o, d))
    return pairs

# ----------------------- Build ROUTES (DB first, then fallbacks) -----------------------
def build_routes() -> List[Tuple[str, str]]:
    # 1) explicit override via ROUTES_CSV like "MEL-BKK;MEL-PNH"
    if ROUTES_CSV:
        pairs = []
        for token in ROUTES_CSV.split(";"):
            token = token.strip().upper()
            if not token:
                continue
            if "-" in token:
                o, d = token.split("-", 1)
                pairs.append((o.strip(), d.strip()))
        if pairs:
            return pairs

    # 2) try Snowflake DIM_SUPPORTED_ROUTES
    try:
        pairs = fetch_supported_routes_from_sf()
        if pairs:
            return pairs
    except Exception as e:
        print(f"[ingestion] WARN could not read DIM_SUPPORTED_ROUTES from Snowflake: {e}")

    # 3) fallback to ORIGIN + DESTINATIONS from env
    return [(ORIGIN, d) for d in DEST_LIST]

ROUTES: List[Tuple[str, str]] = build_routes()

# ----------------------------- Main run loop ----------------------------------
def run_once():
    now = datetime.now(timezone.utc)
    pretty_routes = ", ".join([f"{o}->{d}" for (o, d) in ROUTES])
    print(f"[ingestion] source={SOURCE_NAME} horizon_days={HORIZON_DAYS}")
    print(f"[ingestion] routes={pretty_routes}")
    if STORE_JSON:
        print("[ingestion] raw JSON snapshot storage: ON")

    batch = []
    for origin, dest in ROUTES:
        for i in range(1, HORIZON_DAYS + 1):
            dep = date.today() + timedelta(days=i)
            price, stops, airline, params, raw = fetch_min_price(origin, dest, dep)
            if price is None:
                continue
            batch.append(
                (
                    origin,
                    dest,
                    dep,
                    now,
                    float(price),
                    int(stops) if stops is not None else None,
                    airline,
                    SOURCE_NAME,
                    "Y",
                )
            )
            if STORE_JSON and raw:
                insert_raw_json(f"{origin}-{dest}", json.dumps(params), json.dumps(raw), now)

    n = insert_quotes(batch) if batch else 0
    print(f"Ingestion complete: inserted {n} rows.")
    return n

if __name__ == "__main__":
    run_once()