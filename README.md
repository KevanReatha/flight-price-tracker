# Flight Price Tracker ✈️

An end-to-end data project that simulates a real-world **Analytics Engineering** workflow:
- Ingestion of flight price data from external APIs (Python).
- Storage and modeling in **Snowflake** (warehouse).
- Transformations with **dbt**.
- Interactive analytics with **Streamlit**.

Built fully via a **CLI-first approach** (SnowSQL + Python) to mirror production practices.

---

## 🚀 Tech Stack
- **Snowflake** → cloud data warehouse (storage + compute).
- **dbt** → SQL-based transformations, modular modeling, testing.
- **Python** → ingestion scripts, API integration.
- **Streamlit** → interactive app for insights.
- **Git + GitHub** → version control, CI/CD-ready.

---

## 📂 Repo Structure

flight-price-tracker/
├── sql/                          # SQL scripts run via SnowSQL CLI
│   ├── 01_snowflake_cli_setup.sql   # Create WH, DB, schemas, tables
│   └── 02_check_setup.sql           # Verify setup objects & row counts
│
├── ingestion/                    # Python ingestion (Day 3 onwards)
│   ├── providers/                 # API connectors (Tequila, etc.)
│   ├── utils/                     # Snowflake I/O helpers
│   ├── main.py                    # Entry point for ingestion
│   └── requirements.txt
│
├── dbt/                          # dbt project (Day 4 onwards)
│   ├── models/
│   ├── dbt_project.yml
│   └── profiles.yml.example
│
├── app/                          # Streamlit app (Day 5 onwards)
│   └── streamlit_app.py
│
├── docs/                         # Documentation (diagrams, notes)
│   └── architecture_diagram.png
│
├── .gitignore
├── .env.example
└── README.md

---

## 🛠️ Setup Steps

### Day 1 – Repo Initialization
- Created repo with professional structure.
- Added `.env.example` (Snowflake + API credentials).
- Added `.gitignore` (Python, dbt, envs).

### Day 2 – Snowflake Infra Setup
- Created warehouse `WH_XS` (XSMALL, auto-suspend 60s).
- Created database `FLIGHT_DB` with schemas `RAW`, `STG`, `CORE`, `MART`.
- Created RAW tables:
  - `PRICE_QUOTES_PARSED`
  - `PRICE_QUOTES_JSON`
- All created via **SnowSQL CLI**, not UI.

### Day 3 – Ingestion Pipeline
- Built Python ingestion to call **Tequila API** and fetch flight prices.
- Configurable via `.env`:
  - Horizon of days (default 60).
  - Supported routes (MEL → BKK, PNH, SGN, MNL, HND, ICN).
- Inserted JSON + parsed rows into Snowflake RAW.

### Day 4 – dbt Transformations
- Created dbt project with layers:
  - `STG` → clean parsed rows.
  - `CORE` → aggregated daily minimum price.
  - `MART` → business-ready lowest price by route/date.
- Added schema tests (not_null, unique) for data quality.
- Confirmed dbt run + test pass.

### Day 5 – Streamlit App
- Built interactive app connected to Snowflake:
  - Route/date selectors.
  - Table of lowest prices by route/date.
  - Trend chart of prices over time.
- Cleaned display labels for user-friendliness (Airline, Price AUD).
- Verified end-to-end pipeline (Ingestion → dbt → App).

---

## 👤 Author

**Kevan Tamom** – Analytics Engineer /, building portfolio-ready projects with real-world workflows.  