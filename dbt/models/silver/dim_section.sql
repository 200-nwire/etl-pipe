{{ config(materialized='table') }}

-- Transform lms_enrollments to dim_section (Ed-Fi schema)
-- Specific teaching groups used for rosters and context
-- Note: Using actual column names (group instead of sectionId, course instead of courseId)
select
  -- Make section_id unique by combining group, course, and year
  cast("group" as string) || '_' || cast(course as string) || '_' || cast(year as string) as section_id,
  'lms' as source_system,
  cast("group" as string) as source_section_key,
  cast(null as string) as organization_id,  -- schoolId not in enrollments table
  cast(course as string) as course_id,  -- course instead of courseId
  cast(year as string) || '_default' as term_id,  -- Generate from year
  cast("group" as string) as code,  -- Use group as code
  cast("group" as string) as name,  -- Use group as name
  cast(null as date) as start_date,  -- startDate not available
  cast(null as date) as end_date,  -- endDate not available
  cast(null as string) as delivery_mode,  -- deliveryMode not available
  cast(null as string) as metadata,  -- metadata not directly available
  min(cast(created_on as timestamp)) as created_at,  -- created_on instead of createdAt
  max(cast(created_on as timestamp)) as updated_at  -- Use created_on as fallback
from {{ source('raw_lms', 'lms_enrollments') }}
where "group" is not null and course is not null and year is not null
group by "group", course, year
