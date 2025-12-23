{{ config(materialized='table') }}

-- Transform lms_pages, lms_blocks, lms_lessons to dim_content (unified content dimension)
-- Replaces dim_content_item
-- Union all three content collections
select
  cast(_id as string) as content_id,
  'lms' as source_system,
  cast(_id as string) as source_content_key,
  cast(schoolId as string) as owner_organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  'page' as content_type,
  cast(interactionType as string) as interaction_type,
  cast(title as string) as title,
  cast(description as string) as description,
  cast(difficultyEstimate as float) as difficulty_estimate,
  cast(metadata as string) as metadata,
  cast(createdAt as timestamp) as created_at,
  cast(updatedAt as timestamp) as updated_at
from {{ source('raw_lms', 'lms_pages') }}

union all

select
  cast(_id as string) as content_id,
  'lms' as source_system,
  cast(_id as string) as source_content_key,
  cast(schoolId as string) as owner_organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  'block' as content_type,
  cast(interactionType as string) as interaction_type,
  cast(title as string) as title,
  cast(description as string) as description,
  cast(difficultyEstimate as float) as difficulty_estimate,
  cast(metadata as string) as metadata,
  cast(createdAt as timestamp) as created_at,
  cast(updatedAt as timestamp) as updated_at
from {{ source('raw_lms', 'lms_blocks') }}

union all

select
  cast(_id as string) as content_id,
  'lms' as source_system,
  cast(_id as string) as source_content_key,
  cast(schoolId as string) as owner_organization_id,
  cast(courseId as string) as course_id,
  cast(sectionId as string) as section_id,
  'lesson' as content_type,
  cast(interactionType as string) as interaction_type,
  cast(title as string) as title,
  cast(description as string) as description,
  cast(difficultyEstimate as float) as difficulty_estimate,
  cast(metadata as string) as metadata,
  cast(createdAt as timestamp) as created_at,
  cast(updatedAt as timestamp) as updated_at
from {{ source('raw_lms', 'lms_lessons') }}


