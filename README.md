# Flight Price Tracker (MEL-focused)

**Goal:** Public, data-driven insights on the best time to book popular Melbourne routes (Snowflake + dbt + Streamlit).

## Stack
- Ingestion: Python (APIs) + GitHub Actions (cron)
- Warehouse: Snowflake (XS, auto-suspend)
- Transformations: dbt Core (STG/CORE/MART)
- App: Streamlit (interactive charts)

## Status
- Day 1: Repo scaffold ✅

## Local setup
```bash
python -m venv .venv && source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r ingestion/requirements.txt
pip install -r app/requirements.txt