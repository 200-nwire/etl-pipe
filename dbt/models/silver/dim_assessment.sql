{{ config(materialized='table') }}

select
  cast(assessment_id as string) as assessment_id,
  cast(source_system as string) as source_system,
  cast(source_assessment_key as string) as source_assessment_key,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(content_id as string) as content_id,
  cast(title as string) as title,
  description,
  cast(assessment_type as string) as assessment_type,
  cast(max_score as float64) as max_score,
  cast(passing_score as float64) as passing_score,
  metadata,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_mongo', 'dim_assessment') }}
