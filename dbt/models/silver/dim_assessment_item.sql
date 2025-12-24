{{ config(materialized='table') }}

-- Item dimension for item analysis, psychometrics, and mastery modeling
-- Extracted from lms_exercises (items/questions)
with items_expanded as (
  select
    e._id,
    e.createdAt,
    e.updatedAt,
    unnest(e.items) as item,
    row_number() over (partition by e._id order by (select null)) - 1 as itemIndex
  from {{ source('raw_lms', 'lms_exercises') }} e
  where e.items is not null
)
select
  cast(e._id as string) || '_' || cast(e.itemIndex as string) as item_id,
  'lms' as source_system,
  cast(e._id as string) || '_' || cast(e.itemIndex as string) as source_item_key,
  cast(e._id as string) as assessment_id,
  cast(json_extract_string(e.item, '$.contentId') as string) as content_id,
  cast(json_extract_string(e.item, '$.type') as string) as item_type,
  cast(json_extract_string(e.item, '$.interactionType') as string) as interaction_type,
  cast(substring(json_extract_string(e.item, '$.prompt'), 1, 200) as string) as prompt_summary,
  cast(json_extract(e.item, '$.difficulty') as float) as difficulty_estimate,
  cast(json_extract(e.item, '$.maxScore') as float) as max_score,
  cast(json_extract_string(e.item, '$.metadata') as string) as metadata,
  cast(e.createdAt as timestamp) as created_at,
  cast(e.updatedAt as timestamp) as updated_at
from items_expanded e

