from __future__ import annotations
import os
from typing import List, Tuple, Optional
import snowflake.connector
from datetime import datetime

# -------------------------------------------------------------------
# Connection Helper
# -------------------------------------------------------------------
def _connect():
    """Connect to Snowflake using environment variables."""
    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ.get("SNOWFLAKE_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
    )
    if os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"):
        kwargs["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    elif os.environ.get("SNOWFLAKE_PASSWORD"):
        kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        raise RuntimeError("Provide SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")
    return snowflake.connector.connect(**kwargs)

# -------------------------------------------------------------------
# Idempotent Insert (MERGE)
# -------------------------------------------------------------------
def insert_quotes(
    batch: List[Tuple[str, str, datetime, datetime, float, Optional[int], Optional[str], str]]
) -> int:
    """
    Insert or update parsed quotes into RAW.PRICE_QUOTES_PARSED (idempotent).

    Expected tuple order:
      (origin, destination, departure_date, observed_at, price_aud, stops, airline_code, source)
    """
    if not batch:
        return 0

    dict_rows = []
    for origin, destination, dep_date, observed_at, price_aud, stops, airline, source in batch:
        dict_rows.append(
            {
                "origin": origin,
                "destination": destination,
                "departure_date": dep_date,
                "quote_ts": observed_at,  # matches QUOTE_TS
                "price_aud": float(price_aud),
                "stops": int(stops) if stops is not None else None,
                "airline_code": airline,
                "source": source,
            }
        )

    sql = """
    MERGE INTO FLIGHT_DB.RAW.PRICE_QUOTES_PARSED AS tgt
    USING (
        SELECT
            %(origin)s AS origin,
            %(destination)s AS destination,
            %(departure_date)s AS departure_date,
            %(quote_ts)s AS quote_ts,
            %(price_aud)s AS price_aud,
            %(stops)s AS stops,
            %(airline_code)s AS airline_code,
            %(source)s AS source
    ) AS src
    ON tgt.origin = src.origin
       AND tgt.destination = src.destination
       AND tgt.departure_date = src.departure_date
       AND tgt.quote_ts = src.quote_ts
    WHEN MATCHED THEN
        UPDATE SET
            tgt.price_aud = src.price_aud,
            tgt.stops = src.stops,
            tgt.airline_code = src.airline_code,
            tgt.source = src.source
    WHEN NOT MATCHED THEN
        INSERT (origin, destination, departure_date, quote_ts, price_aud, stops, airline_code, source)
        VALUES (src.origin, src.destination, src.departure_date, src.quote_ts, src.price_aud, src.stops, src.airline_code, src.source);
    """

    with _connect() as con:
        cur = con.cursor()
        try:
            cur.executemany(sql, dict_rows)
            con.commit()
            return len(batch)
        finally:
            cur.close()

# -------------------------------------------------------------------
# Raw JSON insert unchanged
# -------------------------------------------------------------------
def insert_raw_json(
    route_code: str,
    params: dict,
    response: dict,
    ingested_at: datetime,
) -> int:
    """
    Store original API responses in RAW.PRICE_QUOTES_JSON.
    Table columns: ROUTE_CODE (VARCHAR), PARAMS (VARIANT), RESPONSE (VARIANT), INGESTED_AT (TIMESTAMP_TZ)
    """
    sql = """
    INSERT INTO FLIGHT_DB.RAW.PRICE_QUOTES_JSON
      (ROUTE_CODE, PARAMS, RESPONSE, INGESTED_AT)
    VALUES
      (%(route_code)s, %(params)s, %(response)s, %(ingested_at)s)
    """
    with _connect() as con:
        cur = con.cursor()
        try:
            cur.execute(
                sql,
                {
                    "route_code": route_code,
                    "params": params,         # dict -> VARIANT
                    "response": response,     # dict -> VARIANT
                    "ingested_at": ingested_at,
                },
            )
            con.commit()
            return cur.rowcount or 0
        finally:
            cur.close()