{{ config(materialized='table') }}

-- Maps assessment items to skills
-- Extracted from lms_exercises (item metadata)
with items_expanded as (
  select
    e._id,
    unnest(e.items) as item,
    row_number() over (partition by e._id order by (select null)) - 1 as itemIndex
  from {{ source('raw_lms', 'lms_exercises') }} e
  where e.items is not null
),
item_skills as (
  select
    cast(e._id as string) || '_' || cast(e.itemIndex as string) as item_id,
    cast(skillId.value as string) as skill_id
  from items_expanded e
  cross join lateral unnest(json_extract_array(e.item, '$.skills')) as skillId
  where json_extract_array(e.item, '$.skills') is not null
)
select
  item_id || '_' || skill_id as item_skill_map_id,
  item_id,
  skill_id,
  cast(1.0 as float) as weight,
  'authoring' as source,
  cast(null as string) as metadata,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from item_skills
where skill_id is not null

