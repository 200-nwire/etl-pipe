{{ config(materialized='table') }}

-- Transform lms_exercise_submissions to fact_assessment_attempt (Ed-Fi schema)
select
  cast(_id as string) as assessment_attempt_id,
  'lms' as source_system,
  cast(_id as string) as source_attempt_key,
  -- time_id should be joined from dim_time based on attempt_end
  cast(null as int) as time_id,  -- TODO: Join with dim_time
  cast(startedAt as timestamp) as attempt_start,
  cast(submittedAt as timestamp) as attempt_end,
  cast(userId as string) as learner_id,
  cast(sessionId as string) as session_id,
  cast(schoolId as string) as organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(exerciseId as string) as assessment_id,
  cast(exerciseId as string) as content_id,  -- Exercise is also content
  cast(scoreRaw as {{ float_type() }}) as score_raw,
  cast(scoreScaled as {{ float_type() }}) as score_scaled,
  cast(scorePercent as {{ float_type() }}) as score_percent,
  cast(passed as bool) as passed,
  cast(attemptNumber as int) as attempt_number,
  cast(itemCount as int) as item_count,
  cast(completedItemCount as int) as completed_item_count,
  cast(metadata as string) as metadata
from {{ source('raw_lms', 'lms_exercise_submissions') }}
