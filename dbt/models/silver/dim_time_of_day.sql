{{ config(materialized='table') }}

-- Time-of-day dimension for diurnal analysis
-- Optional time-of-day dimension without exploding date dim
-- Generated from all event timestamps
with all_times as (
  select distinct
    extract(hour from timestamp) as hour,
    extract(minute from timestamp) as minute,
    extract(second from timestamp) as second
  from {{ source('raw_lrs', 'lrs_statements') }}
  where timestamp is not null
)
select
  (hour * 3600 + minute * 60 + second) as time_of_day_id,
  cast(hour as int) as hour,
  cast(minute as int) as minute,
  cast(second as int) as second,
  lpad(cast(hour as string), 2, '0') || ':' || 
  lpad(cast(minute as string), 2, '0') || ':' || 
  lpad(cast(second as string), 2, '0') as label
from all_times
order by time_of_day_id

