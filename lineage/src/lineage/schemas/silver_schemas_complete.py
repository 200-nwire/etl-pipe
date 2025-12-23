"""Schema definitions for silver tables with field metadata for Dagster UI."""

from dagster import TableColumn

# Silver table schemas - extracted from silver.dbml
SILVER_TABLE_SCHEMAS = {
    "bridge_assessment_item_map": {
        "description": (
            "Bridge table mapping assessments to assessment items with position and point allocation. "
            "Represents the structure of assessments (which items appear in which order with what point values). "
            "Aligned with Ed-Fi AssessmentItem.AssessmentReference for assessment structure management."
        ),
        "why": (
            "`lms_assessment_profiles` (assessment structure from items array) or `lms_exercise_submissions` "
            "(inferred from submission structure). Enables item-level analytics within assessments, "
            "assessment structure analysis, and point allocation tracking. Maps to Ed-Fi AssessmentItem "
            "concepts for assessment composition."
        ),
        "fields": [
            TableColumn(name="assessment_item_map_id", type="string", description="Stable identifier for the assessment-item mapping. Primary key. Surrogate key for analytics."),
            TableColumn(name="assessment_id", type="string", description="Assessment identifier (foreign key to dim_assessment.assessment_id). Maps to Ed-Fi AssessmentItem.AssessmentReference."),
            TableColumn(name="item_id", type="string", description="Assessment item identifier (foreign key to dim_assessment_item.item_id). Maps to Ed-Fi AssessmentItem.AssessmentItemIdentifier."),
            TableColumn(name="position", type="int", description="Order/position of item within the assessment (1-based). For sequencing and presentation order."),
            TableColumn(name="points", type="float", description="Points allocated to this item within the assessment. For weighted scoring and total point calculation."),
        ]
    },

    "bridge_content_skill_map": {
        "description": (
            "Bridge table mapping content items to skills with weights and sources. "
            "Represents which skills are targeted by which content items and how strongly. "
            "Enables skill-based content recommendations and mastery tracking."
        ),
        "why": (
            "`lms_pages`, `lms_blocks`, `lms_lessons` (extract skill mappings from metadata JSON). "
            "Enables skill-content alignment analysis, content recommendation by skill gaps, "
            "and curriculum mapping. Critical for adaptive learning and skill-based pathways."
        ),
        "fields": [
            TableColumn(name="content_skill_map_id", type="string", description="Stable identifier for the content-skill mapping. Primary key. Surrogate key for analytics."),
            TableColumn(name="content_id", type="string", description="Content item identifier (foreign key to dim_content.content_id). The content that targets this skill."),
            TableColumn(name="skill_id", type="string", description="Skill identifier (foreign key to dim_skill.skill_id). The skill targeted by this content."),
            TableColumn(name="weight", type="float", description="Strength of mapping (0-1). Higher values indicate stronger skill targeting. For weighted skill-content analysis."),
            TableColumn(name="source", type="string", description="Source of mapping: 'authoring' (explicitly set by author), 'manual' (manually curated), 'auto' (automatically inferred). For trust and provenance."),
        ]
    },

    "bridge_course_membership": {
        "description": (
            "Bridge table for course-level memberships when sections are not available. "
            "Represents person-course associations with roles. Aligned with Ed-Fi StudentCourseAssociation "
            "for course-level enrollment tracking."
        ),
        "why": (
            "`lms_enrollments` (aggregated to course level when sectionId is null or not available). "
            "Enables course-level analytics when section granularity is not available. Maps to "
            "Ed-Fi StudentCourseAssociation for course enrollment management. Used as fallback when "
            "dim_section is not populated."
        ),
        "fields": [
            TableColumn(name="course_membership_id", type="string", description="Stable identifier for the course membership. Primary key. Surrogate key for analytics."),
            TableColumn(name="course_id", type="string", description="Course identifier (foreign key to dim_course.course_id). Maps to Ed-Fi StudentCourseAssociation.CourseReference."),
            TableColumn(name="person_id", type="string", description="Person identifier (foreign key to dim_person.person_id). Maps to Ed-Fi StudentCourseAssociation.StudentReference."),
            TableColumn(name="role_key", type="string", description="Membership role (foreign key to ref_membership_role.role_key). Examples: 'Learner', 'Instructor', 'TeachingAssistant'. Maps to Ed-Fi StudentCourseAssociation if available."),
        ]
    },

    "bridge_item_skill_map": {
        "description": (
            "Bridge table mapping assessment items to skills. Represents which skills are measured "
            "by which assessment items. Enables skill-based item analysis and mastery modeling."
        ),
        "why": (
            "`lms_exercises` (extract skill mappings from skills array or metadata). Enables "
            "skill-item alignment analysis, item difficulty by skill, and skill-based assessment "
            "design. Critical for competency-based assessment and skill mastery tracking."
        ),
        "fields": [
            TableColumn(name="item_skill_map_id", type="string", description="Stable identifier for the item-skill mapping. Primary key. Surrogate key for analytics."),
            TableColumn(name="item_id", type="string", description="Assessment item identifier (foreign key to dim_assessment_item.item_id). The item that measures this skill."),
            TableColumn(name="skill_id", type="string", description="Skill identifier (foreign key to dim_skill.skill_id). The skill measured by this item."),
        ]
    },

    "bridge_section_membership": {
        "description": (
            "Bridge table for section memberships with roles and status. Represents person-section "
            "associations (enrollments) with role context. Aligned with Ed-Fi StudentSectionAssociation "
            "for section roster and enrollment management."
        ),
        "why": (
            "`lms_enrollments` (with role information). Enables section-level analytics, roster management, "
            "and role-based access control. Maps to Ed-Fi StudentSectionAssociation.UniqueId and "
            "StudentSectionAssociation.BeginDate/EndDate. Critical for section-level reporting and "
            "teacher-class associations."
        ),
        "fields": [
            TableColumn(name="section_membership_id", type="string", description="Stable membership association identifier. Primary key. Maps to Ed-Fi StudentSectionAssociation.UniqueId."),
            TableColumn(name="section_id", type="string", description="Section identifier (foreign key to dim_section.section_id). Maps to Ed-Fi StudentSectionAssociation.SectionReference."),
            TableColumn(name="person_id", type="string", description="Person identifier (foreign key to dim_person.person_id). Maps to Ed-Fi StudentSectionAssociation.StudentReference."),
            TableColumn(name="role_key", type="string", description="Membership role (foreign key to ref_membership_role.role_key). Examples: 'Learner', 'Instructor', 'TeachingAssistant', 'Proctor', 'Observer'. Maps to Ed-Fi StudentSectionAssociation if available."),
            TableColumn(name="status", type="string", description="Membership status: 'active', 'withdrawn', 'completed'. Maps to Ed-Fi StudentSectionAssociation.EndDate logic (null = active, has date = withdrawn/completed)."),
        ]
    },

    "bridge_skill_standard_map": {
        "description": (
            "Bridge table mapping skills to curriculum standards. Represents alignment between "
            "internal skill taxonomy and external standards frameworks (CCSS, local curriculum, etc.). "
            "Enables standards-based reporting and curriculum alignment analysis."
        ),
        "why": (
            "`lms_rules` (skill-to-standard mappings from rules collection) or external alignment data. "
            "Enables standards-based analytics, curriculum mapping, and compliance reporting. Maps to "
            "Ed-Fi LearningStandard if available. Critical for standards-aligned instruction and "
            "outcome measurement."
        ),
        "fields": [
            TableColumn(name="skill_standard_map_id", type="string", description="Stable identifier for the skill-standard mapping. Primary key. Surrogate key for analytics."),
            TableColumn(name="skill_id", type="string", description="Skill identifier (foreign key to dim_skill.skill_id). The internal skill aligned to this standard."),
            TableColumn(name="standard_id", type="string", description="Standard identifier (foreign key to dim_standard.standard_id). The curriculum standard aligned to this skill."),
        ]
    },

    "dim_academic_term": {
        "description": "Academic calendar/terms for Ed-Fi-aligned slicing and cohort attribution.",
        "why": "`lms_enrollments` (extract term information)",
        "fields": [
            TableColumn(name="term_id", type="string", description="Stable term ID"),
            TableColumn(name="organization_id", type="string", description="Owning org (district/school)"),
            TableColumn(name="school_year", type="string", description="e.g., 2025-2026"),
            TableColumn(name="term_type", type="string", description="semester, quarter, trimester, summer, etc."),
            TableColumn(name="metadata", type="string", description="JSON: calendars, schedules, etc."),
        ]
    },

    "dim_ai_tool": {
        "description": "AI assistant/bot configuration.",
        "why": "`lms_events` (distinct AI tools)",
        "fields": [
            TableColumn(name="ai_tool_id", type="string", description=""),
            TableColumn(name="name", type="string", description=""),
            TableColumn(name="capability_type", type="string", description="tutor/feedback/generator/etc."),
        ]
    },

    "dim_assessment": {
        "description": (
            "Assessment dimension representing logical assessment/test containers. Represents assessments "
            "as collections of items used to measure performance. Aligned with Ed-Fi Assessment entity "
            "for assessment catalog and structure management."
        ),
        "why": (
            "`lms_assessment_profiles` and `lms_exercises` (union, filter exercises with scoring). "
            "Enables assessment-level analytics, gradebook integration, and assessment catalog management. "
            "Maps to Ed-Fi Assessment.AssessmentIdentifier and Assessment.AssessmentTitle. Critical for "
            "assessment performance tracking and learning outcome measurement."
        ),
        "fields": [
            TableColumn(name="assessment_id", type="string", description="Logical assessment/test container identifier. Primary key. Maps to Ed-Fi Assessment.AssessmentIdentifier."),
            TableColumn(name="content_id", type="string", description="Optional content container (foreign key to dim_content.content_id). If assessment is embedded in content. Maps to Ed-Fi Assessment if available."),
            TableColumn(name="assessment_type", type="string", description="Assessment type: 'quiz', 'exam', 'formative', 'summative', 'placement', 'diagnostic', etc. Maps to Ed-Fi Assessment.AssessmentCategory."),
            TableColumn(name="grading_scheme", type="string", description="Grading scheme: 'percent', 'points', 'mastery-bands', 'pass/fail', etc. Maps to Ed-Fi Assessment.ScoringMethod if available."),
            TableColumn(name="max_score", type="float", description="Maximum possible score. For score normalization and percentage calculation."),
            TableColumn(name="passing_score", type="float", description="Passing score threshold. For mastery determination."),
            TableColumn(name="metadata", type="string", description="JSON: Additional assessment metadata (timing, policies, question-set configuration, etc.). Preserves source system context."),
        ]
    },

    "dim_assessment_item": {
        "description": (
            "Assessment item dimension for item analysis, psychometrics, and mastery modeling. "
            "Represents atomic assessment items/questions that can appear in multiple assessments. "
            "Aligned with Ed-Fi AssessmentItem entity for item bank and question management."
        ),
        "why": (
            "`lms_exercises` (items with questions) or derived from assessment structure. Enables "
            "item-level analytics, psychometric analysis, item difficulty tracking, and mastery modeling. "
            "Maps to Ed-Fi AssessmentItem.AssessmentItemIdentifier. Critical for item response theory "
            "(IRT) analysis and adaptive assessment."
        ),
        "fields": [
            TableColumn(name="item_id", type="string", description="Atomic assessment item/question identifier. Primary key. Maps to Ed-Fi AssessmentItem.AssessmentItemIdentifier."),
            TableColumn(name="assessment_id", type="string", description="Primary owning assessment (foreign key to dim_assessment.assessment_id, optional if shared bank). Maps to Ed-Fi AssessmentItem.AssessmentReference."),
            TableColumn(name="content_id", type="string", description="Optional link to content object if aligned (foreign key to dim_content.content_id). For content-embedded items."),
            TableColumn(name="item_type", type="string", description="Item type: 'mcq' (multiple choice), 'short-answer', 'essay', 'numeric', 'drag-drop', etc. Maps to Ed-Fi AssessmentItem.ItemCategory if available."),
            TableColumn(name="prompt_summary", type="string", description="Short safe summary of item prompt; avoid full copyrighted item text. For item identification without exposing full content."),
            TableColumn(name="difficulty_level", type="string", description="Difficulty level: 'easy', 'medium', 'hard', etc. For item difficulty analysis."),
            TableColumn(name="metadata", type="string", description="JSON: Additional item metadata (rubrics, options IDs, bank tags, etc.). Preserves source system context."),
        ]
    },

    "dim_content": {
        "description": (
            "Content/activity dimension representing learning content objects (pages, blocks, lessons). "
            "Unifies all content types into a single dimension for content analytics. Not necessarily "
            "assessment items (use dim_assessment_item for questions). Aligned with Caliper DigitalResource "
            "and xAPI Activity concepts."
        ),
        "why": (
            "`lms_pages`, `lms_blocks`, `lms_lessons` (union all). Enables unified content analytics "
            "across all content types, content engagement tracking, and content recommendation. Maps to "
            "Caliper DigitalResource and xAPI Statement.object (Activity). Critical for content usage "
            "analytics and learning path optimization."
        ),
        "fields": [
            TableColumn(name="content_id", type="string", description="Stable content object identifier. Primary key. Maps to Caliper DigitalResource.id and xAPI Statement.object.id."),
            TableColumn(name="content_type", type="string", description="Content type: 'activity', 'resource', 'video', 'reading', 'interactive', etc. Maps to Caliper DigitalResource.type."),
            TableColumn(name="interaction_type", type="string", description="Interaction model: 'choice', 'numeric', 'essay', 'dragdrop', 'simulation', etc. For interaction analytics."),
            TableColumn(name="title", type="string", description="Content display title. Maps to Caliper DigitalResource.name."),
            TableColumn(name="description", type="string", description="Content description. Maps to Caliper DigitalResource.description if available."),
            TableColumn(name="metadata", type="string", description="JSON: Additional content metadata (tags, media, config, skill mappings, etc.). Preserves source system context."),
        ]
    },

    "dim_course": {
        "description": (
            "Course catalog dimension representing logical courses/planned instruction. "
            "Represents course definitions used across multiple sections. Aligned with Ed-Fi Course "
            "entity for curriculum and course catalog management."
        ),
        "why": (
            "`lms_courses` (direct mapping). Represents planned instruction/course catalog. "
            "Maps to Ed-Fi Course.CourseCode and Course.CourseTitle. Used for course-level analytics "
            "and curriculum tracking. Sections (dim_section) are concrete offerings of courses."
        ),
        "fields": [
            TableColumn(name="course_id", type="string", description="Logical course definition identifier. Primary key. Maps to Ed-Fi Course.CourseCode."),
            TableColumn(name="name", type="string", description="Course title/name. Maps to Ed-Fi Course.CourseTitle."),
            TableColumn(name="subject", type="string", description="Subject area (e.g., 'math', 'language', 'science'). Maps to Ed-Fi Course.AcademicSubject if available."),
            TableColumn(name="grade_band", type="string", description="Grade band (e.g., 'K-2', '3-5', '6-8', '9-12'). Maps to Ed-Fi Course.GradeLevels if available."),
            TableColumn(name="metadata", type="string", description="JSON: Additional course metadata (description, credits, etc.). Preserves source system context."),
        ]
    },

    "dim_date": {
        "description": "Date dimension for reporting. Facts keep exact UTC timestamps; date_id is derived for slicing.",
        "why": "Generated from event timestamps in `lms_events` and `xapi_statements`",
        "fields": [
            TableColumn(name="date_id", type="int", description="Surrogate key for calendar date"),
            TableColumn(name="date", type="date", description="Calendar date (local/org-resolved or UTC date policy)"),
            TableColumn(name="year", type="int", description=""),
            TableColumn(name="month", type="int", description="1-12"),
            TableColumn(name="day", type="int", description="1-31"),
            TableColumn(name="quarter", type="int", description="1-4"),
            TableColumn(name="week_of_year", type="int", description="ISO week number"),
            TableColumn(name="day_of_week", type="int", description="1-7"),
            TableColumn(name="day_name", type="string", description=""),
            TableColumn(name="is_weekend", type="bool", description=""),
            TableColumn(name="is_holiday", type="bool", description=""),
        ]
    },

    "dim_device": {
        "description": "Device dimension for experience, performance, abuse analytics.",
        "why": "`lms_events` and `lms_login` (distinct devices)",
        "fields": [
            TableColumn(name="device_id", type="string", description="Stable device signature id (hashed/fingerprinted)"),
            TableColumn(name="device_type", type="string", description="desktop/tablet/phone"),
        ]
    },

    "dim_lti_tool": {
        "description": "LTI tool integration context.",
        "why": "`lms_events` (distinct LTI tools)",
        "fields": [
            TableColumn(name="lti_tool_id", type="string", description=""),
        ]
    },

    "dim_organization": {
        "description": (
            "Organization dimension representing districts, schools, campuses, and providers. "
            "Supports hierarchical organization structures. Aligned with Ed-Fi EducationOrganization "
            "entity (LocalEducationAgency, School, etc.)."
        ),
        "why": (
            "`lms_schools` (direct mapping). Maintains organization hierarchy via parent_organization_id. "
            "Maps to Ed-Fi EducationOrganization.UniqueId, LocalEducationAgency.LocalEducationAgencyId, "
            "and School.SchoolId. Critical for multi-tenant analytics and organizational reporting."
        ),
        "fields": [
            TableColumn(name="organization_id", type="string", description="Stable organization identifier across platform. Primary key. Maps to Ed-Fi EducationOrganization.UniqueId."),
            TableColumn(name="name", type="string", description="Organization display name. Maps to Ed-Fi EducationOrganization.NameOfInstitution."),
            TableColumn(name="organization_type", type="string", description="Organization type: 'district', 'school', 'campus', 'provider', etc. Maps to Ed-Fi EducationOrganization.EducationOrganizationCategory."),
            TableColumn(name="parent_organization_id", type="string", description="Parent organization ID for hierarchy (root-up). Foreign key to dim_organization. Maps to Ed-Fi EducationOrganization.ParentEducationAgencyReference."),
            TableColumn(name="timezone", type="string", description="IANA default timezone (e.g., 'America/New_York'). For local time conversion. Maps to Ed-Fi School.LocalEducationAgencyReference if available."),
            TableColumn(name="external_ref_uri", type="string", description="Optional canonical URI for interoperability (e.g., Ed-Fi URI). For external system integration."),
            TableColumn(name="metadata", type="string", description="JSON: Additional organization attributes (address, contact info, etc.). Preserves source system context."),
        ]
    },

    "dim_person": {
        "description": (
            "Conformed person dimension following Ed-Fi Person model. Unifies learners, teachers, and staff "
            "into a single dimension to avoid duplication. Use role/membership bridges (bridge_section_membership, "
            "bridge_course_membership) for context-specific roles. Aligned with Ed-Fi Student, Staff, and Person entities."
        ),
        "why": (
            "`lms_users` (filtered by role: student → LEARNER, teacher/staff → TEACHER/STAFF). "
            "Replaces separate dim_learner and dim_teacher tables with conformed person dimension. "
            "Maps to Ed-Fi Student.StudentUniqueId, Staff.StaffUniqueId, and Person.UniqueId. "
            "Enables unified analytics across all person types while maintaining role context via bridges."
        ),
        "fields": [
            TableColumn(name="person_id", type="string", description="Stable person ID across systems (learner/teacher/staff). Primary key. Maps to Ed-Fi Person.UniqueId. Surrogate key for analytics."),
            TableColumn(name="source_system", type="string", description="Source system identifier: 'lms' (MongoDB LMS). For multi-source scenarios."),
            TableColumn(name="source_person_key", type="string", description="Original person key from source system (lms_users._id). For traceability."),
            TableColumn(name="external_user_id", type="string", description="LMS/SIS key for joins to source. Maps to Ed-Fi Student.StudentUniqueId or Staff.StaffUniqueId."),
            TableColumn(name="person_type", type="string", description="Person type: 'LEARNER' | 'TEACHER' | 'STAFF' | 'UNKNOWN'. Derived from lms_users.role array. Maps to Ed-Fi Student/Staff classification."),
            TableColumn(name="full_name", type="string", description="Full display name (first_name + last_name). Avoid for strict PII use cases. Maps to Ed-Fi Person.FirstName + Person.LastSurname."),
            TableColumn(name="given_name", type="string", description="First/given name. Maps to Ed-Fi Person.FirstName. May be null for PII privacy."),
            TableColumn(name="family_name", type="string", description="Last/family name. Maps to Ed-Fi Person.LastSurname. May be null for PII privacy."),
            TableColumn(name="primary_language", type="string", description="Primary language code (e.g., 'he', 'en'). Maps to Ed-Fi Student.Languages if available."),
            TableColumn(name="secondary_language", type="string", description="Secondary language code. Maps to Ed-Fi Student.Languages if available."),
            TableColumn(name="birth_date", type="date", description="Birth date (may be null for privacy). Maps to Ed-Fi Person.BirthDate."),
            TableColumn(name="home_organization_id", type="string", description="Home organization (school/campus). Maps to Ed-Fi Student.SchoolReference or Staff.SchoolReference."),
            TableColumn(name="grade_level", type="string", description="Grade level for learners (e.g., '5', '8', 'K'). Maps to Ed-Fi Student.GradeLevel."),
            TableColumn(name="employment_type", type="string", description="Employment type for staff/teachers. Maps to Ed-Fi Staff.EmploymentStatus if available."),
            TableColumn(name="demographics", type="string", description="JSON: Non-identifying demographic data. Maps to Ed-Fi Student.StudentCharacteristics if available."),
            TableColumn(name="enrollment_status", type="string", description="Enrollment status: 'active', 'withdrawn', etc. (learner-oriented). Maps to Ed-Fi StudentSectionAssociation.EndDate logic."),
            TableColumn(name="metadata", type="string", description="JSON: Additional metadata from HR/SIS/LMS. Preserves source system context."),
            TableColumn(name="created_at", type="timestamp", description="Record creation time. Maps to Ed-Fi Person.CreateDate."),
            TableColumn(name="updated_at", type="timestamp", description="Record last update time. Maps to Ed-Fi Person.LastModifiedDate."),
        ]
    },

    "dim_platform": {
        "description": "Sending/hosting platform dimension.",
        "why": "`lms_events` (distinct platforms)",
        "fields": [
            TableColumn(name="platform_id", type="string", description="Platform or product id (LMS, Player, SIS gateway)"),
            TableColumn(name="name", type="string", description=""),
            TableColumn(name="environment", type="string", description="dev/stg/prod/district"),
        ]
    },

    "dim_section": {
        "description": (
            "Section/class dimension representing concrete teaching groups used for rosters and context. "
            "Sections are specific offerings of courses within terms. Aligned with Ed-Fi Section entity "
            "for class roster and enrollment management."
        ),
        "why": (
            "`lms_enrollments` (grouped by sectionId). Represents concrete class/section offerings. "
            "Maps to Ed-Fi Section.SectionIdentifier and Section.UniqueSectionCode. Critical for "
            "section-level analytics, roster management, and teacher-class associations."
        ),
        "fields": [
            TableColumn(name="section_id", type="string", description="Concrete class/section offering identifier. Primary key. Maps to Ed-Fi Section.SectionIdentifier."),
            TableColumn(name="course_id", type="string", description="Reference to course (foreign key to dim_course). Maps to Ed-Fi Section.CourseOfferingReference."),
            TableColumn(name="term_id", type="string", description="Academic term for this section (foreign key to dim_academic_term). Maps to Ed-Fi Section.SessionReference."),
            TableColumn(name="delivery_mode", type="string", description="Delivery mode: 'online', 'blended', 'in-person', 'hybrid'. Maps to Ed-Fi Section.InstructionalSetting if available."),
            TableColumn(name="section_code", type="string", description="Section code or label. Maps to Ed-Fi Section.UniqueSectionCode."),
            TableColumn(name="section_name", type="string", description="Section display name. Maps to Ed-Fi Section.SectionName."),
            TableColumn(name="metadata", type="string", description="JSON: Additional section metadata (schedules, groups, settings, etc.). Preserves source system context."),
        ]
    },

    "dim_session": {
        "description": "Session dimension for grouping events. Keep exact timestamps in facts.",
        "why": "`lms_login` (session records)",
        "fields": [
            TableColumn(name="session_id", type="string", description="Stable session id (learning session / experience window)"),
            TableColumn(name="person_id", type="string", description="Primary person in session, if single-user session"),
            TableColumn(name="registration_id", type="string", description="xAPI registration / cmi5 registration / LTI session link"),
            TableColumn(name="metadata", type="string", description="JSON flags: experiments, launch params, etc."),
        ]
    },

    "dim_skill": {
        "description": "Skill graph used for mastery, recommendations.",
        "why": "`lms_rules` (filter type='skill')",
        "fields": [
            TableColumn(name="skill_id", type="string", description="Internal skill/competency id"),
            TableColumn(name="name", type="string", description=""),
        ]
    },

    "dim_standard": {
        "description": "Standards/outcomes graph, Ed-Fi/analytics friendly.",
        "why": "`lms_rules` (filter type='standard') or external standards framework",
        "fields": [
            TableColumn(name="standard_id", type="string", description="Curriculum standard / framework node"),
            TableColumn(name="framework", type="string", description="e.g., local curriculum, CCSS, custom"),
        ]
    },

    "dim_time_of_day": {
        "description": (
            "Time-of-day dimension for diurnal analysis without exploding date dimension. "
            "Represents time buckets (hour, minute, second) for analyzing learning patterns by time of day. "
            "Enables analysis of when learners are most active, engagement patterns, and time-based recommendations."
        ),
        "why": (
            "Generated from event timestamps in `lms_events` and `xapi_statements`. Enables diurnal analysis "
            "(learning patterns by time of day) without adding time fields to the date dimension. Useful for "
            "identifying optimal learning times, engagement patterns, and time-based interventions."
        ),
        "fields": [
            TableColumn(name="time_of_day_id", type="int", description="Surrogate key for time-of-day bucket. Primary key. Calculated as hour*3600 + minute*60 + second for efficient joins."),
            TableColumn(name="hour", type="int", description="Hour of day (0-23). For hourly analysis and time-of-day patterns."),
            TableColumn(name="minute", type="int", description="Minute of hour (0-59). For minute-level precision if needed."),
            TableColumn(name="second", type="int", description="Second of minute (0-59). For second-level precision if needed."),
            TableColumn(name="label", type="string", description="Human-readable time label (HH:MM:SS format). For display and reporting."),
        ]
    },

    "fact_ai_interaction": {
        "description": "AI usage telemetry for quality, cost, and pedagogy impact analysis.",
        "why": "`lms_events` (filter aiInteractionId is not null)",
        "fields": [
            TableColumn(name="ai_interaction_id", type="string", description=""),
            TableColumn(name="interaction_timestamp_utc", type="timestamp", description=""),
            TableColumn(name="date_id", type="int", description=""),
            TableColumn(name="ai_tool_id", type="string", description=""),
            TableColumn(name="person_id", type="string", description="Primary human participant"),
            TableColumn(name="counterpart_person_id", type="string", description="Optional second human participant (teacher+learner)"),
            TableColumn(name="interaction_role", type="string", description="user/system/assistant"),
            TableColumn(name="safety_flags", type="string", description="JSON safety classifiers/labels"),
            TableColumn(name="transcript_snippet", type="string", description="Short snippet or anonymized summary; avoid full transcripts if sensitive"),
            TableColumn(name="source_payload", type="string", description="JSON for traces, tool calls, chain metadata"),
        ]
    },

    "fact_assessment_attempt": {
        "description": "Roll-up per assessment execution. Connects naturally to item responses + event stream.",
        "why": "`lms_exercise_submissions`",
        "fields": [
            TableColumn(name="assessment_attempt_id", type="string", description="Stable attempt id"),
            TableColumn(name="date_id", type="int", description="At completion/end for slicing"),
            TableColumn(name="person_id", type="string", description="Learner/person taking the assessment"),
            TableColumn(name="assessment_id", type="string", description=""),
            TableColumn(name="content_id", type="string", description="If assessment is embedded in content container"),
            TableColumn(name="metadata", type="string", description="JSON: accommodations, proctoring, retake reason"),
            TableColumn(name="source_payload", type="string", description="JSON of raw attempt record"),
        ]
    },

    "fact_item_response": {
        "description": "Best-practice grain for item analysis (difficulty, discrimination proxies), mastery modeling, and remediation.",
        "why": "`lms_exercise_submissions` (item-level breakdown) or `lms_events` (item response events)",
        "fields": [
            TableColumn(name="item_response_id", type="string", description="Stable id for item-level response within an attempt"),
            TableColumn(name="response_timestamp_utc", type="timestamp", description=""),
            TableColumn(name="date_id", type="int", description=""),
            TableColumn(name="assessment_attempt_id", type="string", description=""),
            TableColumn(name="person_id", type="string", description=""),
            TableColumn(name="item_id", type="string", description=""),
        ]
    },

    "fact_launch": {
        "description": "Launch events used to connect context across standards and debugging adoption funnels.",
        "why": "`lms_events` (filter ltiLaunchId is not null) or LTI launch events",
        "fields": [
            TableColumn(name="launch_id", type="string", description="Stable id for launch event (LTI/cmi5/custom)"),
            TableColumn(name="launch_timestamp_utc", type="timestamp", description=""),
            TableColumn(name="date_id", type="int", description=""),
            TableColumn(name="registration_id", type="string", description="xAPI/cmi5 registration if available"),
            TableColumn(name="launch_type", type="string", description="LTI_1_3, cmi5, deep_link, direct"),
            TableColumn(name="launch_context", type="string", description="JSON: claims/params"),
        ]
    },

    "fact_learning_event": {
        "description": (
            "Unified atomic event fact table following Caliper Analytics and xAPI specifications. "
            "Supports Caliper: actor/action/object/target/generated/group, and xAPI: actor/verb/object/context/result. "
            "Unifies lms_events and xapi_statements into a single event stream for comprehensive learning analytics. "
            "Aligned with IMS Global Caliper Analytics 1.2 and xAPI 1.0.3 specifications."
        ),
        "why": (
            "`lms_events` + `xapi_statements` (union all via staging layer). Creates unified event stream from "
            "multiple sources (MongoDB LMS events and xAPI LRS statements). Enables cross-platform analytics, "
            "interoperability with Caliper/xAPI-compliant systems, and comprehensive learning activity tracking. "
            "Maps to Caliper Event and xAPI Statement concepts."
        ),
        "fields": [
            TableColumn(name="event_id", type="string", description="Stable identifier for the event row. Primary key. Surrogate key for analytics."),
            TableColumn(name="source_event_key", type="string", description="Raw statement/event ID from source system (lms_events._id or xapi_statements.id). For traceability."),
            TableColumn(name="event_timestamp_utc", type="timestamp", description="Exact timestamp in UTC when the event occurred. Maps to Caliper Event.eventTime and xAPI Statement.timestamp. Used for temporal analysis."),
            TableColumn(name="date_id", type="int", description="Derived date for slicing (foreign key to dim_date.date_id). Enables date-based reporting and cohort analysis."),
            TableColumn(name="time_of_day_id", type="int", description="Optional time-of-day bucket (foreign key to dim_time_of_day.time_of_day_id). For diurnal analysis."),
            TableColumn(name="timezone", type="string", description="Timezone used for local breakdown if applicable (IANA format, e.g., 'America/New_York'). For local time conversion."),
            TableColumn(name="event_type_key", type="string", description="Coarse event bucket (foreign key to ref_event_type.event_type_key). Examples: 'view', 'submit', 'hint', 'navigate', 'launch', 'grade'."),
            TableColumn(name="verb_id", type="int", description="Normalized verb (foreign key to ref_verb.verb_id). Maps to xAPI Statement.verb.id and Caliper Event.action."),
            TableColumn(name="verb_raw", type="string", description="Raw verb ID/IRI if not mapped to normalized verb. Preserves original verb for unmapped cases."),
            TableColumn(name="person_id", type="string", description="Primary actor person (foreign key to dim_person.person_id). Maps to Caliper Event.actor and xAPI Statement.actor."),
            TableColumn(name="actor_type", type="string", description="Actor type: 'learner' | 'teacher' | 'system' | 'ai'. Maps to Caliper Event.actor.type and xAPI Statement.actor.objectType."),
            TableColumn(name="impersonated_person_id", type="string", description="If acting-on-behalf-of (foreign key to dim_person.person_id). For teacher-acting-as-student scenarios. Maps to xAPI Statement.actor (Group with members)."),
            TableColumn(name="membership_id", type="string", description="Optional link to bridge_section_membership.section_membership_id. Provides role context (Learner, Instructor, etc.)."),
            TableColumn(name="object_type_key", type="string", description="Object type (foreign key to ref_object_type.object_type_key). Examples: 'ContentItem', 'AssessmentItem', 'Attempt', 'Annotation'. Maps to Caliper Event.object.type and xAPI Statement.object.objectType."),
            TableColumn(name="object_id", type="string", description="Foreign key by type: content_id (→ dim_content), item_id (→ dim_assessment_item), attempt_id (→ fact_assessment_attempt), etc. Maps to Caliper Event.object.id and xAPI Statement.object.id."),
            TableColumn(name="response", type="string", description="Raw response (hashed/anonymized as needed for privacy). Maps to xAPI Statement.result.response and Caliper Event.object.response."),
            TableColumn(name="duration_seconds", type="int", description="Event duration in seconds if event includes duration. Maps to xAPI Statement.result.duration and Caliper Event.duration."),
            TableColumn(name="event_source", type="string", description="JSON: Original envelope (xAPI statement / Caliper event). Preserved for audit, debugging, and advanced analytics."),
            TableColumn(name="metadata", type="string", description="JSON: Derived/extra fields. Additional context extracted from source events."),
        ]
    },

    "fact_learning_signal": {
        "description": "Computed signals used for adaptivity, dashboards, and early warning.",
        "why": "`lms_events` (filter signalId is not null) or computed from events",
        "fields": [
            TableColumn(name="learning_signal_id", type="string", description=""),
            TableColumn(name="signal_timestamp_utc", type="timestamp", description=""),
            TableColumn(name="date_id", type="int", description=""),
            TableColumn(name="signal_type_key", type="string", description=""),
            TableColumn(name="person_id", type="string", description=""),
            TableColumn(name="metadata", type="string", description="JSON: features, hyperparams, confidence, provenance"),
            TableColumn(name="source_payload", type="string", description="JSON: raw aggregate inputs if needed"),
        ]
    },

    "fact_session_summary": {
        "description": "Session-level aggregates for BI dashboards and cohort trends.",
        "why": "`lms_events` (aggregated by sessionId)",
        "fields": [
            TableColumn(name="session_summary_id", type="string", description=""),
            TableColumn(name="session_id", type="string", description=""),
        ]
    },

    "fact_variant_exposure": {
        "description": "Connects learners to experimental conditions; needed for causal analytics and adaptive audits.",
        "why": "`lms_events` (filter variantExposureId is not null)",
        "fields": [
            TableColumn(name="variant_exposure_id", type="string", description=""),
            TableColumn(name="exposure_timestamp_utc", type="timestamp", description=""),
            TableColumn(name="date_id", type="int", description=""),
            TableColumn(name="person_id", type="string", description=""),
            TableColumn(name="experiment_key", type="string", description=""),
            TableColumn(name="variant_key", type="string", description=""),
            TableColumn(name="randomization_unit", type="string", description="person/session/section"),
        ]
    },

    "ref_event_type": {
        "description": "view/submit/hint/navigate/launch/grade/etc.",
        "why": "`lms_events` (distinct eventType)",
        "fields": [
            TableColumn(name="event_type_key", type="string", description="Coarse event bucket"),
        ]
    },

    "ref_membership_role": {
        "description": "Learner, Instructor, TeachingAssistant, Proctor, Observer, etc.",
        "why": "`lms_enrollments` (distinct roles)",
        "fields": [
            TableColumn(name="role_key", type="string", description=""),
        ]
    },

    "ref_object_type": {
        "description": "ContentItem, AssessmentItem, Attempt, Annotation, LineItem, etc.",
        "why": "`lms_events` and `xapi_statements` (distinct object types)",
        "fields": [
            TableColumn(name="object_type_key", type="string", description="Normalized object type"),
        ]
    },

    "ref_signal_type": {
        "description": "mastery, risk, engagement, confusion, momentum, etc.",
        "why": "`lms_events` (distinct signalTypeKey)",
        "fields": [
            TableColumn(name="signal_type_key", type="string", description=""),
        ]
    },

    "ref_verb": {
        "description": "Normalized verb vocabulary for xAPI/Caliper style actions.",
        "why": "`xapi_statements` and `lms_events` (distinct verbs)",
        "fields": [
            TableColumn(name="verb_id", type="int", description="Surrogate key for normalized verb row"),
            TableColumn(name="iri", type="string", description="Canonical IRI/ID (xAPI/Caliper vocab)"),
            TableColumn(name="short_code", type="string", description="Local code: ANSWERED/COMPLETED/etc."),
        ]
    },

}
