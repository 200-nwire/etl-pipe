{{ config(materialized='table') }}

select
  cast(content_id as string) as content_id,
  cast(source_system as string) as source_system,
  cast(source_content_key as string) as source_content_key,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(section_id as string) as section_id,
  cast(content_type as string) as content_type,
  cast(interaction_type as string) as interaction_type,
  cast(title as string) as title,
  description,
  cast(difficulty_estimate as float64) as difficulty_estimate,
  cast(primary_skill_id as string) as primary_skill_id,
  metadata,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_mongo', 'dim_content_item') }}
