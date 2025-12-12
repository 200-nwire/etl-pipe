{{ config(materialized='table') }}

select
  cast(ai_interaction_id as string) as ai_interaction_id,
  cast(time_id as int) as time_id,
  cast(interaction_timestamp as timestamp) as interaction_timestamp,
  cast(learner_id as string) as learner_id,
  cast(teacher_id as string) as teacher_id,
  cast(session_id as string) as session_id,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(section_id as string) as section_id,
  cast(content_id as string) as content_id,
  cast(skill_id as string) as skill_id,
  cast(ai_tool_id as string) as ai_tool_id,
  cast(interaction_role as string) as interaction_role,
  cast(input_tokens as int64) as input_tokens,
  cast(output_tokens as int64) as output_tokens,
  cast(latency_ms as int64) as latency_ms,
  cast(feedback_rating as float64) as feedback_rating,
  cast(feedback_label as string) as feedback_label,
  transcript_snippet,
  metadata
from {{ source('raw_mongo', 'fact_ai_interaction') }}
