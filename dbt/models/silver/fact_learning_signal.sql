{{ config(materialized='table') }}

-- Transform learning signals from xAPI statements (via staging)
-- Signals are computed analytics derived from events
-- Note: Signal-specific fields not directly available in xAPI, would need to be computed
select
  cast(event_id as string) as learning_signal_id,
  cast(null as int) as time_id,  -- TODO: Join with dim_time
  cast(event_timestamp as timestamp) as signal_timestamp,
  cast(null as string) as signal_type_key,  -- Not available in xAPI
  cast(userId as string) as learner_id,
  cast(schoolId as string) as organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(skillId as string) as skill_id,
  cast(contentId as string) as content_id,
  cast(sessionId as string) as session_id,
  cast(null as float) as signal_value,  -- Not available in xAPI
  cast(null as string) as value_explanation,  -- Not available in xAPI
  cast(null as timestamp) as window_start,  -- Not available in xAPI
  cast(null as timestamp) as window_end,  -- Not available in xAPI
  cast(metadata as string) as metadata
from {{ ref('stg_lrs_events') }}
limit 0  -- Signals not directly available in xAPI, return empty result
