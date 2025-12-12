{{ config(materialized='table') }}

select
  cast(verb_id as int) as verb_id,
  cast(iri as string) as iri,
  cast(short_code as string) as short_code,
  cast(description as string) as description
from {{ source('raw_mongo', 'ref_verb') }}
