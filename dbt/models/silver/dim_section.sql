{{ config(materialized='table') }}

select
  cast(section_id as string) as section_id,
  cast(source_system as string) as source_system,
  cast(source_section_key as string) as source_section_key,
  cast(organization_id as string) as organization_id,
  cast(course_id as string) as course_id,
  cast(code as string) as code,
  cast(name as string) as name,
  cast(term as string) as term,
  cast(start_date as date) as start_date,
  cast(end_date as date) as end_date,
  cast(delivery_mode as string) as delivery_mode,
  metadata,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_mongo', 'dim_section') }}
