{{ config(materialized='table') }}

-- Transform lms_users to dim_person (unified person dimension)
-- Replaces separate dim_learner and dim_teacher tables
-- Note: Using actual MongoDB column names (first_name, last_name, role array, school, created_on, modified_on)
select
  cast(_id as string) as person_id,
  'lms' as source_system,
  cast(_id as string) as source_person_key,
  cast(_id as string) as external_user_id,
  case
    -- Handle role: With normalize=False, dlt preserves arrays as JSON strings in BigQuery
    -- BigQuery JSON functions: JSON_VALUE for scalars, JSON_QUERY for arrays/objects
    -- If role is stored as JSON string array: ["student", "teacher"]
    when JSON_VALUE(SAFE.PARSE_JSON(role), '$[0]') = 'student' 
         or JSON_QUERY(SAFE.PARSE_JSON(role), '$') like '%"student"%' then 'LEARNER'
    when JSON_VALUE(SAFE.PARSE_JSON(role), '$[0]') = 'teacher'
         or JSON_QUERY(SAFE.PARSE_JSON(role), '$') like '%"teacher"%' then 'TEACHER'
    when JSON_VALUE(SAFE.PARSE_JSON(role), '$[0]') = 'staff'
         or JSON_QUERY(SAFE.PARSE_JSON(role), '$') like '%"staff"%' then 'STAFF'
    -- Fallback: if role is a simple string (not JSON array)
    when role = 'student' then 'LEARNER'
    when role = 'teacher' then 'TEACHER'
    when role = 'staff' then 'STAFF'
    else 'UNKNOWN'
  end as person_type,
  cast(concat(coalesce(first_name, ''), ' ', coalesce(last_name, '')) as string) as full_name,
  cast(first_name as string) as given_name,
  cast(last_name as string) as family_name,
  cast(null as string) as primary_language,  -- Not in users table
  cast(null as string) as secondary_language,  -- Not in users table
  cast(null as date) as birth_date,  -- Not in users table
  cast(school as string) as home_organization_id,  -- school instead of schoolId
  cast(grade as string) as grade_level,  -- grade instead of gradeLevel
  cast(null as string) as employment_type,  -- Not in users table
  cast(null as string) as demographics,  -- Not in users table
  cast(null as string) as enrollment_status,  -- status not in users table
  cast(null as string) as metadata,  -- Not in users table
  cast(created_on as timestamp) as created_at,  -- created_on instead of createdAt
  cast(modified_on as timestamp) as updated_at  -- modified_on instead of updatedAt
from {{ source('raw_lms', 'lms_users') }}

