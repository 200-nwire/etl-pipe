{{ config(materialized='table') }}

-- Reference table for normalized object types
-- ContentItem, AssessmentItem, Attempt, Annotation, LineItem, etc.
-- Extracted from xAPI statements (via staging)
select distinct
  cast(objectType as string) as object_type_key,
  cast(objectType as string) as description
from {{ ref('stg_lrs_events') }}
where objectType is not null

