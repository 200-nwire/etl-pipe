{{ config(materialized='table') }}

select
  cast(learner_id as string) as learner_id,
  cast(source_system as string) as source_system,
  cast(source_learner_key as string) as source_learner_key,
  cast(external_user_id as string) as external_user_id,
  cast(full_name as string) as full_name,
  cast(given_name as string) as given_name,
  cast(family_name as string) as family_name,
  cast(grade_level as string) as grade_level,
  cast(primary_language as string) as primary_language,
  cast(secondary_language as string) as secondary_language,
  cast(birth_date as date) as birth_date,
  cast(organization_id as string) as organization_id,
  cast(enrollment_status as string) as enrollment_status,
  demographics,
  cast(learning_profile_id as string) as learning_profile_id,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_mongo', 'dim_learner') }}
