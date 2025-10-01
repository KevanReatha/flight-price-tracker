-- =========================================================
-- Day 2 Verification: Check Snowflake Objects (CLI-safe)
-- Run:
--   snowsql -c myconn -f sql/02_check_setup.sql
-- =========================================================

SELECT '=== Checking Warehouse WH_XS ===' AS step;
SHOW WAREHOUSES LIKE 'WH_XS';

SELECT '=== Checking Database FLIGHT_DB ===' AS step;
SHOW DATABASES LIKE 'FLIGHT_DB';

SELECT '=== Checking Schemas in FLIGHT_DB ===' AS step;
SHOW SCHEMAS IN DATABASE FLIGHT_DB;

SELECT '=== Checking Tables in RAW Schema ===' AS step;
SHOW TABLES IN SCHEMA FLIGHT_DB.RAW;

SELECT '=== Checking row count in RAW.PRICE_QUOTES_PARSED ===' AS step;
SELECT COUNT(*) AS parsed_rows FROM FLIGHT_DB.RAW.PRICE_QUOTES_PARSED;

SELECT '=== Day 2 Verification Completed ===' AS step;