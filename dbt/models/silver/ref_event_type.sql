{{ config(materialized='table') }}

select
  cast(event_type_key as string) as event_type_key,
  cast(description as string) as description
from {{ source('raw_mongo', 'ref_event_type') }}
