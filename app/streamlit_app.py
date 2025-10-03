# app/streamlit_app.py
from __future__ import annotations

from datetime import date, timedelta
import altair as alt
import pandas as pd
import streamlit as st
import snowflake.connector

# --------------------------- Page / Config ---------------------------
st.set_page_config(page_title="Flight Price Tracker", page_icon="✈️", layout="wide")
st.title("✈️ Flight Price Tracker")

SNOW = st.secrets["snowflake"]  # set in .streamlit/secrets.toml

# IATA -> airline names (extend as needed)
AIRLINE_NAME = {
    "JQ": "Jetstar",
    "TR": "Scoot",
    "VJ": "VietJet Air",
    "D7": "AirAsia X",
    "AK": "AirAsia",
    "QF": "Qantas",
    "SQ": "Singapore Airlines",
    "CX": "Cathay Pacific",
    "HX": "Hong Kong Airlines",
    "MU": "China Eastern",
    "HU": "Hainan Airlines",
}

# Visual guardrails for charts
MIN_PRICE, MAX_PRICE = 50, 3000  # AUD


# --------------------------- DB Helpers ---------------------------
def get_connection():
    """Create a Snowflake connection using key-pair or password."""
    kwargs = dict(
        account=SNOW["account"],
        user=SNOW["user"],
        role=SNOW["role"],
        warehouse=SNOW["warehouse"],
        database=SNOW["database"],
        schema=SNOW.get("schema", "MART"),
    )
    if SNOW.get("private_key_path"):
        kwargs["private_key_file"] = SNOW["private_key_path"]
    elif SNOW.get("password"):
        kwargs["password"] = SNOW["password"]
    else:
        st.error("Add `private_key_path` or `password` in .streamlit/secrets.toml")
        st.stop()
    return snowflake.connector.connect(**kwargs)


@st.cache_data(ttl=900)
def fetch_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_connection() as con:
        cur = con.cursor()
        try:
            cur.execute(sql, params or {})
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
        finally:
            cur.close()
    return pd.DataFrame(rows, columns=cols)


# --------------------------- SQL ---------------------------
# Only show the supported routes that actually have data in the selected window
ROUTES_SQL = """
SELECT r.route_code
FROM FLIGHT_DB.CORE.DIM_SUPPORTED_ROUTES r
JOIN FLIGHT_DB.MART.MART_LOWEST_PRICE_BY_ROUTE_DATE m
  ON m.route_code = r.route_code
WHERE m.departure_date BETWEEN %(start)s AND %(end)s
GROUP BY r.route_code
ORDER BY r.route_code
"""

CHEAPEST_NEXT_SQL = """
WITH latest AS (
  SELECT route_code, departure_date, MAX(quote_day) AS last_day
  FROM FLIGHT_DB.MART.MART_LOWEST_PRICE_BY_ROUTE_DATE
  GROUP BY 1,2
)
SELECT
  m.route_code,
  m.departure_date,
  CAST(m.daily_min_price_aud AS FLOAT) AS price_aud,
  m.airline_code,
  m.stops,
  m.quote_day
FROM FLIGHT_DB.MART.MART_LOWEST_PRICE_BY_ROUTE_DATE m
JOIN latest l
  ON m.route_code = l.route_code
 AND m.departure_date = l.departure_date
 AND m.quote_day = l.last_day
WHERE m.route_code = %(route)s
  AND m.departure_date BETWEEN %(start)s AND %(end)s
ORDER BY price_aud ASC
LIMIT 200
"""

TREND_SQL = """
SELECT
  route_code,
  departure_date,
  quote_day,
  CAST(daily_min_price_aud AS FLOAT) AS price_aud,
  airline_code,
  stops
FROM FLIGHT_DB.MART.MART_LOWEST_PRICE_BY_ROUTE_DATE
WHERE route_code = %(route)s
  AND departure_date = %(dep)s
ORDER BY quote_day
"""


# --------------------------- Sidebar / Controls ---------------------------
with st.sidebar:
    today = date.today()
    start_d = st.date_input("Start departure date", today)
    end_d = st.date_input("End departure date", today + timedelta(days=30))
    if start_d > end_d:
        st.error("Start date must be before end date.")
        st.stop()

    if st.button("🔄 Refresh data"):
        st.cache_data.clear()

# Dynamically load routes that have data in window (from your supported list)
routes_df = fetch_df(ROUTES_SQL, {"start": start_d, "end": end_d})
routes = routes_df["ROUTE_CODE"].tolist()

if not routes:
    st.warning(
        "No supported routes have data for this date range. "
        "Run ingestion for these dates or widen the range."
    )
    st.stop()

route = st.sidebar.selectbox("Route", routes, index=0)


# --------------------------- Query + Present ---------------------------
params = {"route": route, "start": start_d, "end": end_d}
raw = fetch_df(CHEAPEST_NEXT_SQL, params)

df = raw.copy()
if not df.empty:
    df["PRICE_AUD"] = pd.to_numeric(df["PRICE_AUD"], errors="coerce")
    df["AIRLINE"] = df["AIRLINE_CODE"].map(AIRLINE_NAME).fillna(df["AIRLINE_CODE"])

st.subheader(f"Cheapest fares for {route} (latest quotes)")
if df.empty:
    st.info("No fares in the selected window.")
else:
    # KPIs (smaller captions for range & freshness)
    k1, k2, k3, k4 = st.columns([1, 1, 1.5, 2])
    min_price = float(df["PRICE_AUD"].min())
    k1.metric("Lowest price", f"AUD {min_price:,.0f}")
    k2.metric("Rows", f"{len(df)}")
    k3.caption(f"**Date range:** {start_d} → {end_d}")
    k4.caption(f"**Last updated (quote day):** {df['QUOTE_DAY'].max()}")

    # User-facing summary table
    df_disp = (
        df[["ROUTE_CODE", "DEPARTURE_DATE", "PRICE_AUD", "AIRLINE", "STOPS"]]
        .rename(columns={
            "ROUTE_CODE": "Route",
            "DEPARTURE_DATE": "Departure date",
            "PRICE_AUD": "Price (AUD)",
            "AIRLINE": "Airline",
            "STOPS": "Stops",
        })
        .copy()
    )
    df_disp["Price (AUD)"] = df_disp["Price (AUD)"].map(lambda x: f"AUD {x:,.0f}")
    st.dataframe(df_disp, use_container_width=True, hide_index=True)

    # --------------------------- Trend section ---------------------------
    st.markdown("### Price trend for a specific departure date (over quote days)")
    pick_date = st.selectbox(
        "Choose a departure date to view its price history:",
        sorted(df["DEPARTURE_DATE"].unique()),
        index=0,
        key="trend_date",
    )

    trend = fetch_df(TREND_SQL, {"route": route, "dep": pick_date})
    if not trend.empty:
        trend["PRICE_AUD"] = pd.to_numeric(trend["PRICE_AUD"], errors="coerce")
        # Visual outlier guard
        trend = trend[(trend["PRICE_AUD"] >= MIN_PRICE) & (trend["PRICE_AUD"] <= MAX_PRICE)]

        tdisp = trend.rename(columns={
            "QUOTE_DAY": "Quote day",
            "PRICE_AUD": "Price_AUD",
            "AIRLINE_CODE": "Airline_Code",
            "STOPS": "Stops",
        })
        tdisp["Airline"] = tdisp["Airline_Code"].map(AIRLINE_NAME).fillna(tdisp["Airline_Code"])

        # Force date-only on X axis & format dd/mm/yy
        tdisp["Quote day"] = pd.to_datetime(tdisp["Quote day"]).dt.date

        chart = (
            alt.Chart(tdisp)
            .mark_line(point=True)
            .encode(
                x=alt.X("Quote day:T", title="Quote day",
                        axis=alt.Axis(format="%d/%m/%y", labelAngle=0)),
                y=alt.Y("Price_AUD:Q", title="Price (AUD)",
                        scale=alt.Scale(zero=False),
                        axis=alt.Axis(format="$.0f")),
                tooltip=[
                    alt.Tooltip("Quote day:T", title="Quote day", format="%d/%m/%y"),
                    alt.Tooltip("Price_AUD:Q", title="Price (AUD)", format="$.0f"),
                    alt.Tooltip("Airline:N", title="Airline"),
                    alt.Tooltip("Stops:Q", title="Stops"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No trend data for that date yet. Run ingestion on multiple days to build history.")

st.caption("Data source: Kiwi Tequila API → Snowflake (RAW/STG/CORE/MART) via dbt. UI: Streamlit + Altair.")