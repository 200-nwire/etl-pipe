{{ config(materialized='table') }}

-- Device dimension for experience, performance, abuse analytics
-- Extracted from xAPI statements (via staging)
-- Note: Device info not directly available in xAPI, would need to be extracted from context extensions
select distinct
  cast(null as string) as device_id,  -- Not available in xAPI
  cast(null as string) as device_type,  -- Not available in xAPI
  cast(null as string) as os,  -- Not available in xAPI
  cast(null as string) as browser,  -- Not available in xAPI
  cast(null as string) as user_agent,  -- Not available in xAPI
  cast(metadata as string) as metadata
from {{ ref('stg_lrs_events') }}
where metadata is not null
limit 0  -- No device data available in xAPI, return empty result

