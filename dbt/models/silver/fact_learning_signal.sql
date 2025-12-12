{{ config(materialized='table') }}

select
  cast(learning_signal_id as string) as learning_signal_id,
  cast(time_id as int) as time_id,
  cast(signal_timestamp as timestamp) as signal_timestamp,
  cast(signal_type_key as string) as signal_type_key,
  cast(learner_id as string) as learner_id,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(section_id as string) as section_id,
  cast(skill_id as string) as skill_id,
  cast(content_id as string) as content_id,
  cast(session_id as string) as session_id,
  cast(signal_value as float64) as signal_value,
  value_explanation,
  cast(window_start as timestamp) as window_start,
  cast(window_end as timestamp) as window_end,
  metadata
from {{ source('raw_mongo', 'fact_learning_signal') }}
