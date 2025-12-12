{{ config(materialized='table') }}

select
  cast(session_summary_id as string) as session_summary_id,
  cast(session_id as string) as session_id,
  cast(learner_id as string) as learner_id,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(section_id as string) as section_id,
  cast(total_events as int) as total_events,
  cast(active_seconds as int) as active_seconds,
  cast(idle_seconds as int) as idle_seconds,
  cast(items_attempted as int) as items_attempted,
  cast(items_mastered as int) as items_mastered,
  cast(hints_requested as int) as hints_requested,
  cast(ai_interactions as int) as ai_interactions,
  cast(signals_generated as int) as signals_generated,
  metadata
from {{ source('raw_mongo', 'fact_session_summary') }}
