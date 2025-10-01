-- =========================================================
-- Day 2 Setup: Warehouse XS, Database, Schemas, RAW Tables
-- Role: ACCOUNTADMIN (ou un rôle avec CREATE privilege)
-- =========================================================

-- 1) Warehouse (XS + autosuspend 60s)
CREATE WAREHOUSE IF NOT EXISTS WH_XS
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE WH_XS;

-- 2) Database + Schemas
CREATE DATABASE IF NOT EXISTS FLIGHT_DB;

CREATE SCHEMA IF NOT EXISTS FLIGHT_DB.RAW;
CREATE SCHEMA IF NOT EXISTS FLIGHT_DB.STG;
CREATE SCHEMA IF NOT EXISTS FLIGHT_DB.CORE;
CREATE SCHEMA IF NOT EXISTS FLIGHT_DB.MART;

USE DATABASE FLIGHT_DB;

-- 3) RAW tables (parsed + optional JSON for audit)
CREATE OR REPLACE TABLE FLIGHT_DB.RAW.PRICE_QUOTES_PARSED (
  ORIGIN         STRING,          -- e.g., MEL
  DESTINATION    STRING,          -- e.g., HKG
  DEPARTURE_DATE DATE,            -- date of flight
  QUOTE_TS       TIMESTAMP_TZ,    -- when quote was fetched
  PRICE_AUD      NUMBER(10,2),    -- price normalized to AUD
  STOPS          INTEGER,         -- 0, 1, 2+
  AIRLINE_CODE   STRING,          -- optional (MVP)
  SOURCE         STRING,          -- 'skyscanner' etc.
  CABIN          STRING           -- 'Y' (Economy)
);

CREATE OR REPLACE TABLE FLIGHT_DB.RAW.PRICE_QUOTES_JSON (
  INGESTED_AT TIMESTAMP_TZ,
  ROUTE_CODE  STRING,    -- e.g., MEL-HKG
  PARAMS      VARIANT,   -- request params snapshot
  RESPONSE    VARIANT    -- raw API response
);

-- 4) Sanity checks
SHOW WAREHOUSES LIKE 'WH_XS';
SHOW DATABASES LIKE 'FLIGHT_DB';
SHOW SCHEMAS IN DATABASE FLIGHT_DB;
SHOW TABLES IN SCHEMA FLIGHT_DB.RAW;

SELECT COUNT(*) AS parsed_rows FROM FLIGHT_DB.RAW.PRICE_QUOTES_PARSED;

-- 5) (Optional) Cost control visibility: recent warehouse usage
-- Lists last 7 days of credits used by WH_XS (approx cost proxy)
SELECT
  DATE_TRUNC('day', START_TIME) AS usage_day,
  SUM(CREDITS_USED)             AS credits_used
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'WH_XS'
  AND START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 1 DESC;