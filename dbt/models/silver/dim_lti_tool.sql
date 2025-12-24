{{ config(materialized='table') }}

-- Transform LTI tools from xAPI statements (via staging)
-- Extract unique LTI tools from lrs_statements
select
  cast(ltiToolId as string) as lti_tool_id,
  cast(platformId as string) as platform_id,
  cast(null as string) as issuer,  -- Not directly available in xAPI
  cast(null as string) as client_id,  -- Not directly available in xAPI
  cast(null as string) as name,  -- Not directly available in xAPI
  cast(null as string) as deployment_id,  -- Not directly available in xAPI
  cast(metadata as string) as metadata
from {{ ref('stg_lrs_events') }}
where ltiToolId is not null
group by ltiToolId, platformId, metadata
