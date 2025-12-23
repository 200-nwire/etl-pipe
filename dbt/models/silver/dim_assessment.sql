{{ config(materialized='table') }}

-- Assessment container (Ed-Fi-ish Assessment)
-- Transform lms_assessment_profiles and lms_exercises to dim_assessment
select
  cast(_id as string) as assessment_id,
  'lms' as source_system,
  cast(_id as string) as source_assessment_key,
  cast(schoolId as string) as organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(termId as string) as term_id,
  cast(contentId as string) as content_id,
  cast(title as string) as title,
  cast(description as string) as description,
  cast(assessmentType as string) as assessment_type,
  cast(maxScore as float) as max_score,
  cast(passingScore as float) as passing_score,
  cast(gradingScheme as string) as grading_scheme,
  cast(metadata as string) as metadata,
  cast(createdAt as timestamp) as created_at,
  cast(updatedAt as timestamp) as updated_at
from {{ source('raw_lms', 'lms_assessment_profiles') }}

union all

select
  cast(_id as string) as assessment_id,
  'lms' as source_system,
  cast(_id as string) as source_assessment_key,
  cast(schoolId as string) as organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  cast(termId as string) as term_id,
  cast(_id as string) as content_id,  -- Exercise is also content
  cast(title as string) as title,
  cast(description as string) as description,
  'exercise' as assessment_type,
  cast(maxScore as float) as max_score,
  cast(passingScore as float) as passing_score,
  cast(null as string) as grading_scheme,
  cast(metadata as string) as metadata,
  cast(createdAt as timestamp) as created_at,
  cast(updatedAt as timestamp) as updated_at
from {{ source('raw_lms', 'lms_exercises') }}
where maxScore is not null  -- Only exercises with scoring
