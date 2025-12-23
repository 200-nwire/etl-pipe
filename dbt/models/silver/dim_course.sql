{{ config(materialized='table') }}

-- Transform lms_courses to dim_course (Ed-Fi schema)
-- Note: Using actual MongoDB column names (caption, description, disciplines, grades, created_on, modified_on)
select
  cast(_id as string) as course_id,
  'lms' as source_system,
  cast(_id as string) as source_course_key,
  cast(null as string) as organization_id,  -- schoolId not in courses table
  cast(null as string) as code,  -- code not in courses table
  cast(caption as string) as name,  -- Use caption as name
  cast(disciplines[1] as string) as subject,  -- Use first discipline as subject
  cast(grades[1] as string) as grade_band,  -- Use first grade as grade_band
  cast(null as string) as academic_year,  -- academicYear not in courses table
  cast(null as string) as metadata,  -- metadata not directly in courses table
  cast(created_on as timestamp) as created_at,  -- created_on instead of createdAt
  cast(modified_on as timestamp) as updated_at  -- modified_on instead of updatedAt
from {{ source('raw_lms', 'lms_courses') }}
