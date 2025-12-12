{{ config(materialized='table') }}

select
  cast(teacher_id as string) as teacher_id,
  cast(source_system as string) as source_system,
  cast(source_teacher_key as string) as source_teacher_key,
  cast(external_user_id as string) as external_user_id,
  cast(full_name as string) as full_name,
  cast(given_name as string) as given_name,
  cast(family_name as string) as family_name,
  cast(primary_subject as string) as primary_subject,
  cast(organization_id as string) as organization_id,
  cast(employment_type as string) as employment_type,
  metadata,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_mongo', 'dim_teacher') }}
