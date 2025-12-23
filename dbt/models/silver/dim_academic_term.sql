{{ config(materialized='table') }}

-- Academic calendar/terms for Ed-Fi-aligned slicing and cohort attribution
-- Note: Term data not available in lms_enrollments raw table
-- Creating synthetic terms from year field if available
select distinct
  cast(year as string) || '_default' as term_id,  -- Generate term_id from year
  'lms' as source_system,
  cast(year as string) || '_default' as source_term_key,
  cast(null as string) as organization_id,  -- schoolId not in enrollments
  cast(year as string) as school_year,
  cast('default' as string) as term_type,  -- termType not available
  cast('Default Term ' || year as string) as name,
  cast(null as date) as start_date,  -- startDate not available
  cast(null as date) as end_date,  -- endDate not available
  cast(null as string) as metadata,
  min(cast(created_on as timestamp)) as created_at,
  max(cast(created_on as timestamp)) as updated_at
from {{ source('raw_lms', 'lms_enrollments') }}
where year is not null
group by year

