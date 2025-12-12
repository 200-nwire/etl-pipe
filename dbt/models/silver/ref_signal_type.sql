{{ config(materialized='table') }}

select
  cast(signal_type_key as string) as signal_type_key,
  cast(description as string) as description,
  cast(category as string) as category
from {{ source('raw_mongo', 'ref_signal_type') }}
