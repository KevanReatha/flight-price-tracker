with base as (
  select
    route_code,
    origin,
    destination,
    departure_date,
    date_trunc('day', quote_ts) as quote_day,
    quote_ts,
    price_aud,
    stops,
    airline_code,
    source,
    cabin
  from {{ ref('stg_price_quotes_parsed') }}
),

-- pick lowest price per day/route/departure
ranked as (
  select
    *,
    row_number() over (
      partition by route_code, departure_date, quote_day
      order by price_aud asc, quote_ts desc
    ) as rn
  from base
)

select
  route_code,
  origin,
  destination,
  departure_date,
  quote_day,
  price_aud as daily_min_price_aud,
  stops,
  airline_code,
  source,
  cabin,
  quote_ts as observed_at
from ranked
where rn = 1