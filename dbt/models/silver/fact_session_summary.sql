{{ config(materialized='table') }}

-- Aggregate session summaries from xAPI statements (via staging)
-- Summarize events per session
select
  cast(sessionId as string) as session_summary_id,
  cast(sessionId as string) as session_id,
  cast(userId as string) as learner_id,
  cast(schoolId as string) as organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(count(*) as int) as total_events,
  cast(sum(durationSeconds) as int) as active_seconds,  -- Using duration as proxy for active time
  cast(null as int) as idle_seconds,  -- Not available in xAPI
  cast(count(distinct contentId) as int) as items_attempted,
  cast(count(distinct case when isCorrect = true then contentId end) as int) as items_mastered,
  cast(0 as int) as hints_requested,  -- Not available in xAPI
  cast(sum(case when aiToolId is not null then 1 else 0 end) as int) as ai_interactions,
  cast(0 as int) as signals_generated,  -- Not available in xAPI
  cast(max(metadata) as string) as metadata
from {{ ref('stg_lrs_events') }}
where sessionId is not null
group by sessionId, userId, schoolId, courseId, sectionId
