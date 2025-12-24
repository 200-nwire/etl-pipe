{{ config(materialized='table') }}

-- Connects internal skills to external curriculum standards
-- Extracted from lms_rules (skill-to-standard mappings) or external alignment
with skill_standards as (
  select
    cast(skillId as string) as skill_id,
    cast(standardId as string) as standard_id,
    cast(weight as float) as weight,
    cast(source as string) as source
  from {{ source('raw_lms', 'lms_rules') }},
  unnest(json_extract_array(metadata, '$.standardMappings')) as mapping
  where type = 'skill' and metadata is not null
)
select
  skill_id || '_' || standard_id as skill_standard_map_id,
  skill_id,
  standard_id,
  coalesce(weight, 1.0) as weight,
  coalesce(source, 'manual') as source,
  cast(null as string) as metadata,
  current_timestamp() as created_at,
  current_timestamp() as updated_at
from skill_standards
where skill_id is not null and standard_id is not null


