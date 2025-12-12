{{ config(materialized='table') }}

select
  cast(lti_tool_id as string) as lti_tool_id,
  cast(platform_id as string) as platform_id,
  cast(issuer as string) as issuer,
  cast(client_id as string) as client_id,
  cast(name as string) as name,
  cast(deployment_id as string) as deployment_id,
  metadata
from {{ source('raw_mongo', 'dim_lti_tool') }}
