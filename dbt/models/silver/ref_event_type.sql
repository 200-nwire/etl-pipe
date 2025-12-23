{{ config(materialized='table') }}

-- Extract unique event types from xAPI statements (via staging)
select
  cast(eventType as string) as event_type_key,
  cast(verbDescription as string) as description
from {{ ref('stg_lrs_events') }}
where eventType is not null
group by eventType, verbDescription
