{{ config(materialized='table') }}

-- Standards/outcomes graph, Ed-Fi/analytics friendly
-- Extracted from lms_rules where type = 'standard'
select
  cast(_id as string) as standard_id,
  cast(framework as string) as framework,
  cast(externalCode as string) as external_code,
  cast(name as string) as name,
  cast(description as string) as description,
  cast(gradeBand as string) as grade_band,
  cast(subject as string) as subject,
  cast(parentStandardId as string) as parent_standard_id,
  cast(metadata as string) as metadata,
  cast(createdAt as timestamp) as created_at,
  cast(updatedAt as timestamp) as updated_at
from {{ source('raw_lms', 'lms_rules') }}
where type = 'standard'


