{{ config(materialized='table') }}

-- Transform AI tools from xAPI statements (via staging)
-- Extract unique AI tools from lrs_statements
-- Note: AI tool info may be in xAPI context extensions or metadata
select
  cast(aiToolId as string) as ai_tool_id,
  cast(null as string) as name,  -- Not available in xAPI, would need to be extracted from context
  cast(null as string) as provider,  -- Not available in xAPI
  cast(null as string) as model_name,  -- Not available in xAPI
  cast(null as string) as capability_type,  -- Not available in xAPI
  cast(metadata as string) as metadata
from {{ ref('stg_lrs_events') }}
where aiToolId is not null
group by aiToolId, metadata
