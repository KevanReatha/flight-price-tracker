# common/snow.py
from __future__ import annotations
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def connect_snowflake(
    account: str | None = None,
    user: str | None = None,
    role: str | None = None,
    warehouse: str | None = None,
    database: str | None = None,
    schema: str | None = None,
) -> snowflake.connector.SnowflakeConnection:
    """
    Robust Snowflake connection:
      - Prefers key-pair auth (inline PEM or file), falls back to password
      - Converts PEM -> PKCS8 DER bytes (fresh JWT each call)
    """
    cfg = {
        "account": account or os.environ["SNOWFLAKE_ACCOUNT"],
        "user": user or os.environ["SNOWFLAKE_USER"],
        "role": role or os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": database or os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": schema or os.environ.get("SNOWFLAKE_SCHEMA"),
    }

    pem_inline = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PEM")   # for Streamlit secrets
    pem_path   = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")  # for local/Prefect
    password   = os.environ.get("SNOWFLAKE_PASSWORD")          # fallback

    if pem_inline:
        private_key = serialization.load_pem_private_key(
            pem_inline.encode(), password=None, backend=default_backend()
        )
        cfg["private_key"] = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    elif pem_path:
        with open(pem_path, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(), password=None, backend=default_backend()
            )
        cfg["private_key"] = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    elif password:
        cfg["password"] = password
    else:
        raise RuntimeError(
            "Missing auth: set SNOWFLAKE_PRIVATE_KEY_PEM or SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD."
        )

    return snowflake.connector.connect(**cfg)