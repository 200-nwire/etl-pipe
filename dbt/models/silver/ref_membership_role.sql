{{ config(materialized='table') }}

-- Reference table for membership roles
-- Maps role codes to descriptions (Learner, Instructor, TeachingAssistant, Proctor, Observer, etc.)
select
  cast(role as string) as role_key,
  case
    when role = 'student' then 'Learner'
    when role = 'teacher' then 'Instructor'
    when role = 'staff' then 'TeachingAssistant'
    when role = 'admin' then 'Administrator'
    when role = 'observer' then 'Observer'
    when role = 'proctor' then 'Proctor'
    else 'Unknown'
  end as description
from {{ source('raw_lms', 'lms_users') }}
where role is not null
group by role


