# orchestration/prefect_flow.py
from prefect import flow, task, get_run_logger
import os
import subprocess

# Import your existing ingestion entrypoint
from ingestion.main import run_once

@task(retries=2, retry_delay_seconds=60)
def ingest_task() -> int:
    """
    Runs the ingestion once. Retries on failure.
    Returns: number of inserted rows.
    """
    inserted = run_once()  # make sure run_once() returns n
    return inserted or 0

@task
def dbt_task():
    """
    Runs dbt models + tests. Fails the flow if dbt fails.
    """
    env = os.environ.copy()
    # ensure dbt finds your Snowflake profile
    env["DBT_PROFILES_DIR"] = os.path.expanduser("~/.dbt")

    # 1) Transform
    subprocess.run(
        ["dbt", "run", "--project-dir", "dbt", "--target", "dev"],
        check=True, env=env
    )
    # 2) Tests
    subprocess.run(
        ["dbt", "test", "--project-dir", "dbt", "--target", "dev"],
        check=True, env=env
    )

@flow(name="flight-price-tracker-daily")
def daily_flow():
    """
    Orchestrates the daily pipeline:
      - Ingestion from Tequila -> Snowflake RAW
      - dbt transforms + tests (STG -> CORE -> MART)
    """
    logger = get_run_logger()
    inserted = ingest_task()
    logger.info(f"[prefect] ingestion inserted rows: {inserted}")
    dbt_task()
    logger.info("[prefect] dbt run+test complete")

if __name__ == "__main__":
    # ad-hoc run (no schedule)
    daily_flow()