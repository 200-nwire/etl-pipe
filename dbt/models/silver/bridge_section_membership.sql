{{ config(materialized='table') }}

-- Roster/assignment bridge: replaces StudentSectionAssociation + StaffSectionAssociation patterns
-- Extracted from lms_enrollments
-- Note: Using actual MongoDB column names (group instead of sectionId, student instead of userId)
-- Role comes from users table join
select
  cast(e._id as string) as section_membership_id,
  'lms' as source_system,
  cast(e._id as string) as source_membership_key,
  cast(e."group" as string) as section_id,  -- group is the section identifier
  cast(e.student as string) as person_id,  -- student instead of userId
  coalesce(
    case
      when u.role = 'student' then 'Learner'
      when u.role = 'teacher' then 'Instructor'
      when u.role = 'staff' then 'TeachingAssistant'
      else 'Learner'
    end,
    'Learner'
  ) as role_key,
  cast(null as date) as begin_date,  -- startDate not available
  cast(null as date) as end_date,  -- endDate not available
  cast(e.status as string) as status,
  cast(null as string) as metadata,  -- metadata not directly available
  cast(e.created_on as timestamp) as created_at,
  cast(e.modified_on as timestamp) as updated_at
from {{ source('raw_lms', 'lms_enrollments') }} e
left join {{ source('raw_lms', 'lms_users') }} u
  on e.student = u._id
where e."group" is not null and e.student is not null

