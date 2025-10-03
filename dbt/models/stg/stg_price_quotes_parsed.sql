with src as (

    select
        cast(ORIGIN as string)            as origin,
        cast(DESTINATION as string)       as destination,
        cast(DEPARTURE_DATE as date)      as departure_date,
        cast(QUOTE_TS as timestamp_tz)    as quote_ts,
        cast(PRICE_AUD as number(10,2))   as price_aud,
        cast(STOPS as int)                as stops,
        cast(AIRLINE_CODE as string)      as airline_code,
        cast(SOURCE as string)            as source,
        cast(CABIN as string)             as cabin
    from {{ source('raw', 'PRICE_QUOTES_PARSED') }}

)

select
  origin,
  destination,
  origin || '-' || destination          as route_code,
  departure_date,
  quote_ts,
  price_aud,
  coalesce(stops, 0)                    as stops,
  airline_code,
  source,
  cabin
from src