{{ config(materialized='table') }}

select
  cast(skill_id as string) as skill_id,
  cast(external_skill_code as string) as external_skill_code,
  cast(name as string) as name,
  description,
  cast(domain as string) as domain,
  cast(strand as string) as strand,
  cast(level as string) as level,
  cast(parent_skill_id as string) as parent_skill_id,
  metadata,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_mongo', 'dim_skill') }}
