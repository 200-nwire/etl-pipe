{{ config(materialized='table') }}

-- Maps content objects to skills for tagging and inference
-- Extracted from lms_pages, lms_blocks, lms_lessons metadata
with content_skills as (
  select
    cast(_id as string) as content_id,
    cast(skillId as string) as skill_id,
    cast(skillWeight as float) as weight,
    'authoring' as source
  from {{ source('raw_lms', 'lms_pages') }},
  unnest(json_extract_array(metadata, '$.skills')) as skill
  where metadata is not null
  
  union all
  
  select
    cast(_id as string) as content_id,
    cast(skillId as string) as skill_id,
    cast(skillWeight as float) as weight,
    'authoring' as source
  from {{ source('raw_lms', 'lms_blocks') }},
  unnest(json_extract_array(metadata, '$.skills')) as skill
  where metadata is not null
  
  union all
  
  select
    cast(_id as string) as content_id,
    cast(skillId as string) as skill_id,
    cast(skillWeight as float) as weight,
    'authoring' as source
  from {{ source('raw_lms', 'lms_lessons') }},
  unnest(json_extract_array(metadata, '$.skills')) as skill
  where metadata is not null
)
select
  content_id || '_' || skill_id as content_skill_map_id,
  content_id,
  skill_id,
  coalesce(weight, 1.0) as weight,
  source,
  cast(null as string) as metadata,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from content_skills
where skill_id is not null


