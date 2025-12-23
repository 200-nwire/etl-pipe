{{ config(materialized='table') }}

-- Best-practice grain for item analysis (difficulty, discrimination proxies), mastery modeling, and remediation
-- Extracted from lms_exercise_submissions (item-level breakdown)
select
  cast(_id as string) || '_' || cast(itemId as string) as item_response_id,
  'lms' as source_system,
  cast(_id as string) || '_' || cast(itemId as string) as source_response_key,
  cast(responseTime as timestamp) as response_timestamp_utc,
  cast(extract(epoch from date(responseTime))::int / 86400 as int) as date_id,
  cast((extract(hour from responseTime) * 3600 + extract(minute from responseTime) * 60 + extract(second from responseTime)) as int) as time_of_day_id,
  cast(_id as string) as assessment_attempt_id,
  cast(userId as string) as person_id,
  cast(sessionId as string) as session_id,
  cast(schoolId as string) as organization_id,
  cast(termId as string) as term_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(null as string) as membership_id,
  cast(exerciseId as string) as assessment_id,
  cast(itemId as string) as item_id,
  cast(itemContentId as string) as content_id,
  cast(itemSkillId as string) as skill_id,
  cast(null as string) as standard_id,
  cast(attemptNumber as int) as attempt_number,
  cast(itemPosition as int) as item_position,
  cast(response as string) as response,
  cast(isCorrect as boolean) as is_correct,
  cast(itemScore as float) as score_raw,
  cast(itemScoreScaled as float) as score_scaled,
  cast(latencyMs as int) as latency_ms,
  cast(hintsUsed as int) as hints_used,
  cast(metadata as string) as metadata,
  cast(sourcePayload as string) as source_payload
from {{ source('raw_lms', 'lms_exercise_submissions') }},
unnest(itemResponses) as itemResponse
where itemResponses is not null


