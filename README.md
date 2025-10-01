# Flight Price Tracker ✈️

An end-to-end data project that simulates a real-world **Analytics Engineering** workflow:
- Ingestion of flight price data from external APIs (Python).
- Storage and modeling in **Snowflake** (warehouse).
- Transformations with **dbt**.
- Interactive analytics with **Streamlit**.

Built fully via **CLI-first approach** (SnowSQL + Python) to mirror production practices.

---

## 🚀 Tech Stack
- **Snowflake** → cloud data warehouse (storage + compute).
- **dbt** → SQL-based transformations, modular modeling.
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
│   ├── providers/                 # API connectors (Skyscanner, etc.)
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

**Commands:**
```bash
snowsql -c myconn -f sql/01_snowflake_cli_setup.sql
snowsql -c myconn -f sql/02_check_setup.sql


👤 Author

Kevan Tamom – aspiring Analytics Engineer / AI Engineer, building portfolio-ready projects with real-world workflows.