{{ config(materialized='table') }}

-- Transform skills from lms_rules or create from skill definitions
-- Note: Skills may be embedded in rules/config, adjust based on actual schema
select
  cast(_id as string) as skill_id,
  cast(externalCode as string) as external_skill_code,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(domain as string) as domain,
  cast(strand as string) as strand,
  cast(level as string) as level,
  cast(parentSkillId as string) as parent_skill_id,
  cast(metadata as string) as metadata,
  cast(createdAt as timestamp) as created_at,
  cast(updatedAt as timestamp) as updated_at
from {{ source('raw_lms', 'lms_rules') }}
where type = 'skill'  -- Filter for skill rules, adjust based on actual schema
