{{ config(materialized='table') }}

select
  cast(session_id as string) as session_id,
  cast(source_system as string) as source_system,
  cast(source_session_key as string) as source_session_key,
  cast(learner_id as string) as learner_id,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(section_id as string) as section_id,
  cast(platform_id as string) as platform_id,
  cast(device_type as string) as device_type,
  cast(user_agent as string) as user_agent,
  cast(ip_hash as string) as ip_hash,
  cast(session_start as timestamp) as session_start,
  cast(session_end as timestamp) as session_end,
  cast(session_duration_seconds as int64) as session_duration_seconds,
  metadata
from {{ source('raw_mongo', 'dim_session') }}
