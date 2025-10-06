# orchestration/prefect_flow.py
from __future__ import annotations

import os
import subprocess
from pathlib import Path

from prefect import flow, task, get_run_logger
from snowflake.connector.errors import DatabaseError

# ──────────────────────────────────────────────────────────────────────────────
# Circuit breaker (prevents repeated bad logins from re-locking your user)
# ──────────────────────────────────────────────────────────────────────────────
CIRCUIT_FILE = Path(".sf_circuit_open")

def _circuit_open() -> bool:
    return CIRCUIT_FILE.exists()

def _open_circuit() -> None:
    CIRCUIT_FILE.write_text("opened")

def _close_circuit() -> None:
    if CIRCUIT_FILE.exists():
        CIRCUIT_FILE.unlink()


# ──────────────────────────────────────────────────────────────────────────────
# Ingestion
# ──────────────────────────────────────────────────────────────────────────────
# Import your existing ingestion entrypoint lazily (keeps scheduler light)
@task(retries=2, retry_delay_seconds=[60, 180], name="run-ingestion")
def ingest_task() -> int:
    """
    Runs the ingestion once. Retries on transient failures.
    IMPORTANT: If the underlying code raises a Snowflake auth/lock/MFA error,
    we DO NOT want infinite retries (handled by the flow's circuit breaker).
    """
    from ingestion.main import run_once  # local import on task run

    inserted = run_once()  # make sure run_once() returns an int
    return int(inserted or 0)


# ──────────────────────────────────────────────────────────────────────────────
# dbt transforms + tests
# ──────────────────────────────────────────────────────────────────────────────
@task(name="dbt-run-and-test")
def dbt_task():
    """
    Runs dbt models + tests. Fails the flow if dbt fails.
    Uses your local ~/.dbt/profiles.yml (dev profile).
    """
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = os.path.expanduser("~/.dbt")

    # Transform
    subprocess.run(
        ["dbt", "run", "--project-dir", "dbt", "--target", "dev"],
        check=True,
        env=env,
    )

    # Tests
    subprocess.run(
        ["dbt", "test", "--project-dir", "dbt", "--target", "dev"],
        check=True,
        env=env,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Orchestration flow
# ──────────────────────────────────────────────────────────────────────────────
@flow(name="flight-price-tracker-daily")
def daily_flow():
    """
    Orchestrates the daily pipeline:
      1) Ingestion -> Snowflake RAW
      2) dbt run + dbt test (STG -> CORE -> MART)
    Circuit breaker will skip runs after hard auth/lock errors until fixed.
    """
    log = get_run_logger()

    # Safety first: if we recently saw an auth lock, skip to avoid re-locking.
    if _circuit_open():
        log.warning("Circuit OPEN — skipping ingestion to avoid Snowflake lockouts.")
        return

    # Ingestion with auth-aware handling
    try:
        inserted = ingest_task()
        log.info(f"[prefect] ingestion inserted rows: {inserted}")
        _close_circuit()  # success closes the breaker if it was open
    except DatabaseError as e:
        msg = str(e).lower()
        # Fail-fast on authentication/lock/MFA signals and open the circuit.
        if (
            "incorrect username or password" in msg
            or "temporarily locked" in msg
            or "mfa" in msg
            or "authentication" in msg
        ):
            log.error("Auth/lock error detected; opening circuit breaker for safety.")
            _open_circuit()
        # Re-raise so Prefect marks this run as failed (and respects task retries)
        raise

    # Only run dbt if ingestion phase completed
    dbt_task()
    log.info("[prefect] dbt run+test complete")


if __name__ == "__main__":
    # Ad-hoc local execution (no schedule)
    daily_flow()