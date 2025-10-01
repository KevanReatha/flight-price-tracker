import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def _load_private_key_bytes(path: str):
    with open(path, "rb") as f:
        key = serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def get_conn():
    # Why: Key-pair auth avoids interactive MFA prompts and is safer than passwords.
    pk_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
    )
    if pk_path:
        kwargs["private_key"] = _load_private_key_bytes(pk_path)
    else:
        kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]  # fallback only

    return snowflake.connector.connect(**kwargs)

def insert_quotes(rows):
    """
    rows: [(ORIGIN, DESTINATION, DEPARTURE_DATE, QUOTE_TS, PRICE_AUD, STOPS, AIRLINE_CODE, SOURCE, CABIN), ...]
    """
    if not rows:
        return 0
    sql = """
    INSERT INTO RAW.PRICE_QUOTES_PARSED
    (ORIGIN, DESTINATION, DEPARTURE_DATE, QUOTE_TS, PRICE_AUD, STOPS, AIRLINE_CODE, SOURCE, CABIN)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
    return len(rows)

def insert_raw_json(route_code, params_json, response_json, ingested_at):
    # Why: keeping raw snapshots helps audit parsing bugs & ToS compliance (publish aggregates, keep raw private).
    sql = """
    INSERT INTO RAW.PRICE_QUOTES_JSON (INGESTED_AT, ROUTE_CODE, PARAMS, RESPONSE)
    VALUES (%s, %s, PARSE_JSON(%s), PARSE_JSON(%s))
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (ingested_at, route_code, params_json, response_json))