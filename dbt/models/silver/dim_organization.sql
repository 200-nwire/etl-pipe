{{ config(materialized='table') }}

select
  cast(organization_id as string) as organization_id,
  cast(source_system as string) as source_system,
  cast(source_organization_key as string) as source_organization_key,
  cast(name as string) as name,
  cast(organization_type as string) as organization_type,
  cast(parent_organization_id as string) as parent_organization_id,
  cast(country_code as string) as country_code,
  cast(region as string) as region,
  cast(timezone as string) as timezone,
  metadata,
  cast(created_at as timestamp) as created_at,
  cast(updated_at as timestamp) as updated_at
from {{ source('raw_mongo', 'dim_organization') }}
