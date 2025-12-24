{{ config(materialized='table') }}

-- Transform lms_schools to dim_organization (Ed-Fi schema)
-- Districts, schools, campuses, providers
-- Note: Using actual MongoDB column names (created_on, modified_on, location, semel)
select
  cast(_id as string) as organization_id,
  'lms' as source_system,
  cast(_id as string) as source_organization_key,
  cast(name as string) as name,
  cast('school' as string) as organization_type,  -- type not in schools table, default to 'school'
  cast(null as string) as parent_organization_id,  -- parentId not in schools table
  cast(null as string) as country_code,  -- countryCode not in schools table
  cast(location as string) as region,  -- Use location as region
  cast(null as string) as timezone,  -- timezone not in schools table
  cast(null as string) as external_ref_uri,  -- externalRefUri not in schools table
  cast(null as string) as metadata,  -- metadata not directly in schools table
  cast(created_on as timestamp) as created_at,  -- created_on instead of createdAt
  cast(modified_on as timestamp) as updated_at  -- modified_on instead of updatedAt
from {{ source('raw_lms', 'lms_schools') }}
