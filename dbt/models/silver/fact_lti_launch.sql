{{ config(materialized='table') }}

select
  cast(lti_launch_id as string) as lti_launch_id,
  cast(time_id as int) as time_id,
  cast(launch_timestamp as timestamp) as launch_timestamp,
  cast(session_id as string) as session_id,
  cast(learner_id as string) as learner_id,
  cast(teacher_id as string) as teacher_id,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(section_id as string) as section_id,
  cast(lti_tool_id as string) as lti_tool_id,
  cast(platform_id as string) as platform_id,
  launch_context,
  metadata
from {{ source('raw_mongo', 'fact_lti_launch') }}
