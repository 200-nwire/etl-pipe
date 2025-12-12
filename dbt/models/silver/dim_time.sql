{{ config(materialized='table') }}

select
  cast(time_id as int) as time_id,
  cast(date as date) as date,
  cast(year as int) as year,
  cast(month as int) as month,
  cast(day as int) as day,
  cast(quarter as int) as quarter,
  cast(week_of_year as int) as week_of_year,
  cast(day_of_week as int) as day_of_week,
  cast(day_name as string) as day_name,
  cast(is_weekend as bool) as is_weekend,
  cast(hour as int) as hour,
  cast(minute as int) as minute,
  cast(timezone as string) as timezone
from {{ source('raw_mongo', 'dim_time') }}
