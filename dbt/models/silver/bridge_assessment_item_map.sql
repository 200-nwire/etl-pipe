{{ config(materialized='table') }}

-- Assessment structure mapping; supports item analysis and blueprinting
-- Extracted from lms_assessment_profiles (structure) or lms_exercise_submissions (inferred)
with assessment_items as (
  select
    cast(_id as string) as assessment_id,
    cast(itemId as string) as item_id,
    cast(position as int) as position,
    cast(points as float) as points
  from {{ source('raw_lms', 'lms_assessment_profiles') }},
  unnest(items) as item
  where items is not null
  
  union all
  
  select distinct
    cast(exerciseId as string) as assessment_id,
    cast(itemId as string) as item_id,
    cast(itemPosition as int) as position,
    cast(itemPoints as float) as points
  from {{ source('raw_lms', 'lms_exercise_submissions') }}
  where itemId is not null
)
select
  assessment_id || '_' || item_id as assessment_item_map_id,
  assessment_id,
  item_id,
  min(position) as position,
  min(points) as points,
  cast(null as string) as metadata,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from assessment_items
group by assessment_id, item_id


