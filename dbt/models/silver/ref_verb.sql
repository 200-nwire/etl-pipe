{{ config(materialized='table') }}

-- Extract unique verbs from xAPI statements
-- Verbs come from xAPI standard vocabulary
-- Uses JSON extraction from JSON columns
select
  cast(JSON_VALUE(verb, '$.id') as string) as verb_id,
  cast(JSON_VALUE(verb, '$.id') as string) as iri,
  cast(null as string) as short_code,  -- Not available in xAPI verb structure
  cast(JSON_VALUE(verb, '$.display.en-US') as string) as description
from (
  select distinct
    JSON_VALUE(verb, '$.id') as verbId,
    JSON_VALUE(verb, '$.display.en-US') as verbDescription
  from {{ source('raw_lrs', 'lrs_statements') }}
  where JSON_VALUE(verb, '$.id') is not null
)
