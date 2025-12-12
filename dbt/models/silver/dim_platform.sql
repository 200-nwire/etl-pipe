{{ config(materialized='table') }}

select
  cast(platform_id as string) as platform_id,
  cast(name as string) as name,
  cast(vendor as string) as vendor,
  cast(version as string) as version,
  cast(environment as string) as environment,
  metadata
from {{ source('raw_mongo', 'dim_platform') }}
