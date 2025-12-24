{{ config(materialized='table') }}

-- Transform xAPI statements to dim_session (Ed-Fi schema)
-- Sessions are derived from xAPI statements grouped by sessionId
select
  cast(sessionId as string) as session_id,
  'xapi' as source_system,
  cast(sessionId as string) as source_session_key,
  cast(userId as string) as learner_id,
  cast(schoolId as string) as organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(platformId as string) as platform_id,
  cast(null as string) as device_type,  -- Not available in xAPI
  cast(null as string) as user_agent,  -- Not available in xAPI
  cast(null as string) as ip_hash,  -- Not available in xAPI
  min(cast(event_timestamp as timestamp)) as session_start,
  max(cast(event_timestamp as timestamp)) as session_end,
  cast(sum(durationSeconds) as int64) as session_duration_seconds,
  cast(max(metadata) as string) as metadata
from {{ ref('stg_lrs_events') }}
where sessionId is not null
group by sessionId, userId, schoolId, courseId, sectionId, platformId
