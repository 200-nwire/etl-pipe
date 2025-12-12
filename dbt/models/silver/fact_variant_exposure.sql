{{ config(materialized='table') }}

select
  cast(variant_exposure_id as string) as variant_exposure_id,
  cast(time_id as int) as time_id,
  cast(exposure_timestamp as timestamp) as exposure_timestamp,
  cast(learner_id as string) as learner_id,
  cast(session_id as string) as session_id,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(section_id as string) as section_id,
  cast(content_id as string) as content_id,
  cast(skill_id as string) as skill_id,
  cast(experiment_key as string) as experiment_key,
  cast(variant_key as string) as variant_key,
  cast(is_control as bool) as is_control,
  metadata
from {{ source('raw_mongo', 'fact_variant_exposure') }}
