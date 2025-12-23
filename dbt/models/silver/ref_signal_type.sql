{{ config(materialized='table') }}

-- Reference table for learning signal types
-- Note: Signal types not directly available in xAPI, return empty result
select
  cast(null as string) as signal_type_key,
  cast(null as string) as description,
  cast(null as string) as category
from {{ ref('stg_lrs_events') }}
limit 0  -- Signal types not available in xAPI, return empty result
