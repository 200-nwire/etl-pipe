{{ config(materialized='table') }}

select
  cast(assessment_attempt_id as string) as assessment_attempt_id,
  cast(source_system as string) as source_system,
  cast(source_attempt_key as string) as source_attempt_key,
  cast(time_id as int) as time_id,
  cast(attempt_start as timestamp) as attempt_start,
  cast(attempt_end as timestamp) as attempt_end,
  cast(learner_id as string) as learner_id,
  cast(session_id as string) as session_id,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(section_id as string) as section_id,
  cast(assessment_id as string) as assessment_id,
  cast(content_id as string) as content_id,
  cast(score_raw as {{ float_type() }}) as score_raw,
  cast(score_scaled as {{ float_type() }}) as score_scaled,
  cast(score_percent as {{ float_type() }}) as score_percent,
  cast(passed as bool) as passed,
  cast(attempt_number as int) as attempt_number,
  cast(item_count as int) as item_count,
  cast(completed_item_count as int) as completed_item_count,
  metadata
from {{ source('raw_mongo', 'fact_assessment_attempt') }}
