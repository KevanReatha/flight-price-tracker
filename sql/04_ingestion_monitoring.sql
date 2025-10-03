-- 1) volume by quote_day
SELECT quote_day, COUNT(*) AS quotes
FROM FLIGHT_DB.MART.MART_LOWEST_PRICE_BY_ROUTE_DATE
GROUP BY quote_day
ORDER BY quote_day DESC;

-- 2) routes with cheapest fares (last 7 quote days)
WITH recent AS (
  SELECT *
  FROM FLIGHT_DB.MART.MART_LOWEST_PRICE_BY_ROUTE_DATE
  WHERE quote_day >= DATEADD('day', -7, CURRENT_DATE())
)
SELECT route_code, departure_date, MIN(daily_min_price_aud) AS min_price_aud
FROM recent
GROUP BY route_code, departure_date
ORDER BY min_price_aud ASC
LIMIT 20;

-- 3) latest snapshot-style view: most recent quote_day per route+date
WITH latest AS (
  SELECT route_code, departure_date, MAX(quote_day) AS last_day
  FROM FLIGHT_DB.MART.MART_LOWEST_PRICE_BY_ROUTE_DATE
  GROUP BY 1,2
)
SELECT m.*
FROM FLIGHT_DB.MART.MART_LOWEST_PRICE_BY_ROUTE_DATE m
JOIN latest l
  ON m.route_code = l.route_code
 AND m.departure_date = l.departure_date
 AND m.quote_day = l.last_day
ORDER BY m.daily_min_price_aud ASC
LIMIT 50;