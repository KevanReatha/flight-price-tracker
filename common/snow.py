# common/snow.py
from __future__ import annotations
import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

try:
    import streamlit as st  # available on Streamlit Cloud
except Exception:
    st = None


def _pem_to_pkcs8_der(pem_str: str) -> bytes:
    """Convert a PEM string to PKCS8 DER bytes for Snowflake connector."""
    key = serialization.load_pem_private_key(
        pem_str.encode(), password=None, backend=default_backend()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


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
      - On Streamlit Cloud: reads st.secrets["snowflake"]
      - Locally/Prefect:    reads environment variables
      - Prefers key-pair auth (PEM inline or file); falls back to password
    """
    # 1) Prefer Streamlit secrets when available
    secrets = None
    if st is not None:
        try:
            secrets = st.secrets.get("snowflake", None)
        except Exception:
            secrets = None

    if secrets:
        cfg = {
            "account": account or secrets.get("account"),
            "user":    user    or secrets.get("user"),
            "role":    role    or secrets.get("role"),
            "warehouse": warehouse or secrets.get("warehouse"),
            "database":  database  or secrets.get("database"),
            "schema":    schema    or secrets.get("schema"),
        }

        private_key_pem = secrets.get("private_key")  # your current secrets name
        password        = secrets.get("password")     # optional fallback

        if private_key_pem:
            cfg["private_key"] = _pem_to_pkcs8_der(private_key_pem)
        elif password:
            cfg["password"] = password
        else:
            raise RuntimeError(
                "snowflake secrets found, but neither 'private_key' nor 'password' provided."
            )

        return snowflake.connector.connect(**cfg)

    # 2) Fallback to environment variables (your original behavior)
    cfg = {
        "account": account or os.environ["SNOWFLAKE_ACCOUNT"],
        "user": user or os.environ["SNOWFLAKE_USER"],
        "role": role or os.environ.get("SNOWFLAKE_ROLE"),
        "warehouse": warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "database": database or os.environ.get("SNOWFLAKE_DATABASE"),
        "schema": schema or os.environ.get("SNOWFLAKE_SCHEMA"),
    }

    pem_inline = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PEM")   # inline PEM
    pem_path   = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")  # path to PEM file
    password   = os.environ.get("SNOWFLAKE_PASSWORD")          # fallback

    if pem_inline:
        cfg["private_key"] = _pem_to_pkcs8_der(pem_inline)
    elif pem_path:
        with open(pem_path, "rb") as f:
            cfg["private_key"] = _pem_to_pkcs8_der(f.read().decode())
    elif password:
        cfg["password"] = password
    else:
        raise RuntimeError(
            "Missing auth: set SNOWFLAKE_PRIVATE_KEY_PEM or SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD."
        )

    return snowflake.connector.connect(**cfg)