{{ config(materialized='table') }}

-- Date dimension for reporting
-- Generated from all event timestamps in lrs_statements
-- Facts keep exact UTC timestamps; date_id is derived for slicing
-- Note: lms_events removed - MongoDB doesn't have events collection
with all_dates as (
  select distinct
    date(timestamp) as date
  from {{ source('raw_lrs', 'lrs_statements') }}
  where timestamp is not null
)
select
  cast(extract(epoch from date)::int / 86400 as int) as date_id,
  date as date,
  cast(extract(year from date) as int) as year,
  cast(extract(month from date) as int) as month,
  cast(extract(day from date) as int) as day,
  cast(extract(quarter from date) as int) as quarter,
  cast(extract(week from date) as int) as week_of_year,
  cast(extract(dow from date) as int) + 1 as day_of_week,
  to_char(date, 'Day') as day_name,
  case when extract(dow from date) in (0, 6) then true else false end as is_weekend,
  false as is_holiday,
  cast(null as string) as holiday_name
from all_dates
order by date

