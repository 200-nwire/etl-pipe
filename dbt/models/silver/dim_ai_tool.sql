{{ config(materialized='table') }}

select
  cast(ai_tool_id as string) as ai_tool_id,
  cast(name as string) as name,
  cast(provider as string) as provider,
  cast(model_name as string) as model_name,
  cast(capability_type as string) as capability_type,
  metadata
from {{ source('raw_mongo', 'dim_ai_tool') }}
