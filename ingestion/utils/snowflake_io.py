# ingestion/utils/snowflake_io.py
import os, time
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError, OperationalError

def _conn_kwargs():
    """Build minimal kwargs from env (key-pair preferred, password fallback)."""
    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "WH_XS"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "FLIGHT_DB"),
    )
    # Key-pair first
    key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if key_path:
        kwargs["private_key_file"] = key_path
    else:
        # Only if you intentionally use a password
        pwd = os.environ.get("SNOWFLAKE_PASSWORD")
        if not pwd:
            raise RuntimeError("No SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD set.")
        kwargs["password"] = pwd
    return kwargs

def connect_snowflake(max_retries=3):
    """
    Connect with conservative retry:
    - Immediately abort on credential/user errors (prevents lockout).
    - Back off on transient 5xx/network errors only.
    """
    transient_codes = {"240001", "250001"}  # network / gateway-ish
    for attempt in range(max_retries):
        try:
            return snowflake.connector.connect(**_conn_kwargs())
        except DatabaseError as e:
            code = getattr(e, "errno", None)
            msg = str(e).lower()

            # === Hard-stop: credential & lockout signals (no retry) ===
            if ("incorrect username or password" in msg or
                "mfa" in msg or
                "temporarily locked" in msg or
                "authentication" in msg or
                code in {"390100", "390106"}):
                raise

            # === Retry only transient network/service hiccups ===
            if code and str(code) in transient_codes:
                time.sleep(1 + attempt)
                continue

            # Unknown/other: don't spin
            raise

    # If we get here, all retries failed
    raise RuntimeError("Snowflake connection failed after retries.")