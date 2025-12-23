{{ config(materialized='table') }}

-- Transform platform info from xAPI statements (via staging)
-- Extract unique platforms from lrs_statements
select
  cast(platformId as string) as platform_id,
  cast(null as string) as name,  -- Not directly available in xAPI
  cast(null as string) as vendor,  -- Not directly available in xAPI
  cast(null as string) as version,  -- Not directly available in xAPI
  cast(null as string) as environment,  -- Not directly available in xAPI
  cast(metadata as string) as metadata
from {{ ref('stg_lrs_events') }}
where platformId is not null
group by platformId, metadata
