{{ config(materialized='table') }}

-- Transform AI interactions from xAPI statements (via staging)
-- Filter for AI-related events (where aiToolId is not null)
select
  cast(event_id as string) as ai_interaction_id,
  cast(null as int) as time_id,  -- TODO: Join with dim_time
  cast(event_timestamp as timestamp) as interaction_timestamp,
  cast(userId as string) as learner_id,
  cast(null as string) as teacher_id,  -- Not available in xAPI
  cast(sessionId as string) as session_id,
  cast(schoolId as string) as organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(contentId as string) as content_id,
  cast(skillId as string) as skill_id,
  cast(aiToolId as string) as ai_tool_id,
  cast(null as string) as interaction_role,  -- Not available in xAPI
  cast(null as int64) as input_tokens,  -- Not available in xAPI
  cast(null as int64) as output_tokens,  -- Not available in xAPI
  cast(latencyMs as int64) as latency_ms,
  cast(null as float) as feedback_rating,  -- Not available in xAPI
  cast(null as string) as feedback_label,  -- Not available in xAPI
  cast(null as string) as transcript_snippet,  -- Not available in xAPI
  cast(metadata as string) as metadata
from {{ ref('stg_lrs_events') }}
where aiToolId is not null  -- Filter for AI interaction events
