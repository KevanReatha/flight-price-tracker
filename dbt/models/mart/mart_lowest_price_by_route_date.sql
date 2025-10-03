select
  route_code,
  origin,
  destination,
  departure_date,
  quote_day,
  daily_min_price_aud,
  stops,
  airline_code,
  source,
  cabin,
  observed_at
from {{ ref('core_daily_quotes') }}