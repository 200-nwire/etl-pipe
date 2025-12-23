{{ config(materialized='table') }}

-- Optional: membership at course level when sections are not available
-- Aggregated from lms_enrollments to course level
-- Note: Using actual MongoDB column names (student, course, group)
-- Role comes from users table join
select distinct
  cast(e.course as string) || '_' || cast(e.student as string) as course_membership_id,
  'lms' as source_system,
  cast(e.course as string) || '_' || cast(e.student as string) as source_membership_key,
  cast(e.course as string) as course_id,
  cast(e.student as string) as person_id,
  coalesce(
    case
      when u.role = 'student' then 'Learner'
      when u.role = 'teacher' then 'Instructor'
      when u.role = 'staff' then 'TeachingAssistant'
      else 'Learner'
    end,
    'Learner'
  ) as role_key,
  cast(null as date) as begin_date,  -- startDate not available in enrollments
  cast(null as date) as end_date,  -- endDate not available in enrollments
  cast(max(e.status) as string) as status,
  cast(null as string) as metadata,
  min(cast(e.created_on as timestamp)) as created_at,
  max(cast(e.modified_on as timestamp)) as updated_at
from {{ source('raw_lms', 'lms_enrollments') }} e
left join {{ source('raw_lms', 'lms_users') }} u
  on e.student = u._id
where e.course is not null and e.student is not null
group by e.course, e.student, u.role

