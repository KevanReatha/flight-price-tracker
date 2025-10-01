-- =========================================================
-- Ingestion Test: insert a dummy row + verify + cleanup
-- Run with:
--   snowsql -c myconn -f sql/03_ingestion_test.sql
-- =========================================================

-- 1) Banner
SELECT '=== Inserting one dummy row into RAW.PRICE_QUOTES_PARSED ===' AS step;

-- 2) Insert (use a clearly-marked SOURCE so we can clean up)
INSERT INTO FLIGHT_DB.RAW.PRICE_QUOTES_PARSED
  (ORIGIN, DESTINATION, DEPARTURE_DATE, QUOTE_TS, PRICE_AUD, STOPS, AIRLINE_CODE, SOURCE, CABIN)
VALUES
  ('MEL','HKG', DATEADD(day, 60, CURRENT_DATE()), CURRENT_TIMESTAMP(), 555.00, 0, 'XX', 'ingestion_test', 'Y');

-- 3) Verify count increased
SELECT '=== Row count after insert (should be >= 1) ===' AS step;
SELECT COUNT(*) AS parsed_rows FROM FLIGHT_DB.RAW.PRICE_QUOTES_PARSED;

-- 4) Show the last inserted test row
SELECT '=== Last test row (SOURCE = ingestion_test) ===' AS step;
SELECT *
FROM FLIGHT_DB.RAW.PRICE_QUOTES_PARSED
WHERE SOURCE = 'ingestion_test'
ORDER BY QUOTE_TS DESC
LIMIT 1;

-- 5) (Optional) also test JSON bucket
SELECT '=== Inserting one dummy JSON payload into RAW.PRICE_QUOTES_JSON ===' AS step;
INSERT INTO FLIGHT_DB.RAW.PRICE_QUOTES_JSON (INGESTED_AT, ROUTE_CODE, PARAMS, RESPONSE)
VALUES (
  CURRENT_TIMESTAMP(),
  'MEL-HKG',
  PARSE_JSON('{"curr":"AUD","stops":0,"provider":"test"}'),
  PARSE_JSON('{"min_price":555,"airline":"XX"}')
);

-- 6) Verify JSON row exists
SELECT '=== Last JSON row ===' AS step;
SELECT *
FROM FLIGHT_DB.RAW.PRICE_QUOTES_JSON
ORDER BY INGESTED_AT DESC
LIMIT 1;

-- 7) Cleanup (uncomment to remove the dummy rows)
-- SELECT '=== Cleanup: deleting test rows (uncomment to run) ===' AS step;
-- DELETE FROM FLIGHT_DB.RAW.PRICE_QUOTES_PARSED WHERE SOURCE = 'ingestion_test';
-- DELETE FROM FLIGHT_DB.RAW.PRICE_QUOTES_JSON WHERE ROUTE_CODE = 'MEL-HKG' AND RESPONSE:"airline"::string = 'XX';