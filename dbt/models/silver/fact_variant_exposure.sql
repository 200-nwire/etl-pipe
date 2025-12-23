{{ config(materialized='table') }}

-- Transform A/B test variant exposures from xAPI statements (via staging)
-- Note: Variant exposure data not directly available in xAPI, would need to be in context extensions
select
  cast(event_id as string) as variant_exposure_id,
  cast(null as int) as time_id,  -- TODO: Join with dim_time
  cast(event_timestamp as timestamp) as exposure_timestamp,
  cast(userId as string) as learner_id,
  cast(sessionId as string) as session_id,
  cast(schoolId as string) as organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(contentId as string) as content_id,
  cast(skillId as string) as skill_id,
  cast(null as string) as experiment_key,  -- Not available in xAPI
  cast(null as string) as variant_key,  -- Not available in xAPI
  cast(null as bool) as is_control,  -- Not available in xAPI
  cast(metadata as string) as metadata
from {{ ref('stg_lrs_events') }}
limit 0  -- Variant exposure data not available in xAPI, return empty result
