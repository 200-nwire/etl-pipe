{{ config(materialized='table') }}

select
  cast(course_id as string) as course_id,
  cast(source_system as string) as source_system,
  cast(source_course_key as string) as source_course_key,
  cast(organization_id as string) as organization_id,
  cast(code as string) as code,
  cast(name as string) as name,
  cast(subject as string) as subject,
  cast(grade_band as string) as grade_band,
  cast(academic_year as string) as academic_year,
  metadata,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_mongo', 'dim_course') }}
