{{ config(materialized='table') }}

-- Launch events used to connect context across standards and debugging adoption funnels
-- Extracted from xAPI statements (via staging) where ltiToolId IS NOT NULL
select
  cast(sessionId || '_' || cast(event_timestamp as string) as string) as launch_id,
  'xapi' as source_system,
  cast(event_timestamp as timestamp) as launch_timestamp_utc,
  cast(extract(epoch from date(event_timestamp))::int / 86400 as int) as date_id,
  cast((extract(hour from event_timestamp) * 3600 + extract(minute from event_timestamp) * 60 + extract(second from event_timestamp)) as int) as time_of_day_id,
  cast(platformId as string) as platform_id,
  cast(ltiToolId as string) as lti_tool_id,
  cast(sessionId as string) as session_id,
  cast(null as string) as registration_id,  -- Not available in xAPI
  cast(userId as string) as person_id,
  cast(schoolId as string) as organization_id,
  cast(null as string) as term_id,  -- Not available in xAPI
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(null as string) as membership_id,
  cast('lti' as string) as launch_type,  -- Default to LTI for xAPI launches
  cast(null as string) as launch_context,  -- Not available in xAPI
  cast(metadata as string) as metadata,
  cast(fullStatement as string) as source_payload
from {{ ref('stg_lrs_events') }}
where ltiToolId is not null

