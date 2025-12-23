"""Schema definitions for raw tables with field metadata for Dagster UI."""

from dagster import MetadataValue, TableSchema, TableColumn

# Raw table schemas with field definitions
RAW_TABLE_SCHEMAS = {
    "lms_users": {
        "description": "User accounts and profiles from MongoDB LMS. Contains learners, teachers, and staff with role-based filtering. Aligned with Ed-Fi Student/Staff entities.",
        "why": "Source for unified dim_person dimension. Replaces separate learner/teacher tables with conformed person dimension following Ed-Fi Person model.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique user identifier (MongoDB ObjectId converted to string). Maps to Ed-Fi Person.UniqueId."),
            TableColumn(name="first_name", type="string", description="First/given name. Maps to Ed-Fi Person.FirstName. May be null for PII privacy."),
            TableColumn(name="last_name", type="string", description="Last/family name. Maps to Ed-Fi Person.LastSurname. May be null for PII privacy."),
            TableColumn(name="role", type="string", description="User role array stored as JSON string: ['student'], ['teacher'], ['staff'], etc. Used to determine person_type (LEARNER/TEACHER/STAFF). Maps to Ed-Fi Student/Staff classification."),
            TableColumn(name="grade", type="integer", description="Grade level as integer (e.g., 5, 8). For learners only. Maps to Ed-Fi Student.GradeLevel."),
            TableColumn(name="gradeNumber", type="integer", description="Numeric representation of grade level. Alternative to grade field."),
            TableColumn(name="school", type="string", description="Reference to school/organization (ObjectId converted to string). Maps to Ed-Fi Student.SchoolReference."),
            TableColumn(name="schools", type="string", description="JSON array of school IDs associated with the user (for multi-school scenarios)."),
            TableColumn(name="id_number", type="string", description="National ID number or similar identifier. Maps to Ed-Fi Person.IdentificationCodes."),
            TableColumn(name="sis_id", type="integer", description="Student Information System ID. Maps to Ed-Fi Student.StudentUniqueId."),
            TableColumn(name="gender", type="string", description="Gender of the user. Maps to Ed-Fi Person.Sex."),
            TableColumn(name="coins", type="integer", description="User's current coin balance (gamification currency)."),
            TableColumn(name="onboarding_status", type="integer", description="Current status in onboarding flow (0=not started, 1=in progress, 2=completed)."),
            TableColumn(name="messaging_ready", type="boolean", description="Indicates if user is ready for messaging features."),
            TableColumn(name="deleted", type="boolean", description="Flag indicating if the user record is soft-deleted."),
            TableColumn(name="deletedAt", type="timestamp", description="Timestamp when the record was soft-deleted (null if active)."),
            TableColumn(name="username", type="string", description="User's login username. Maps to Ed-Fi Person.LoginId."),
            TableColumn(name="password", type="string", description="User's hashed password (never exposed in analytics)."),
            TableColumn(name="avatar", type="string", description="URL or path to user's avatar image."),
            TableColumn(name="sso_id", type="string", description="Single Sign-On identifier for external authentication."),
            TableColumn(name="ems_id", type="string", description="External Management System ID for integration."),
            TableColumn(name="created_on", type="timestamp", description="Record creation time. Maps to Ed-Fi Person.CreateDate."),
            TableColumn(name="modified_on", type="timestamp", description="Record last update time. Maps to Ed-Fi Person.LastModifiedDate. Used for incremental loading."),
            TableColumn(name="__v", type="integer", description="Mongoose version key for optimistic concurrency control."),
        ]
    },
    "lms_schools": {
        "description": "School and organization records from MongoDB LMS. Supports hierarchical organization structures.",
        "why": "Direct source for dim_organization. Maintains organization hierarchy via parentId.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique school identifier (ObjectId converted to string)"),
            TableColumn(name="name", type="string", description="School/organization display name"),
            TableColumn(name="type", type="string", description="Organization type: 'district', 'school', 'campus', etc."),
            TableColumn(name="parentId", type="string", description="Parent organization ID (for hierarchy)"),
            TableColumn(name="countryCode", type="string", description="ISO 3166-1 alpha-2 country code"),
            TableColumn(name="region", type="string", description="State, region, or municipality"),
            TableColumn(name="timezone", type="string", description="Default timezone (IANA format)"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_courses": {
        "description": "Course catalog from MongoDB LMS. Logical course definitions used across sections.",
        "why": "Direct source for dim_course. Represents planned instruction/course catalog.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique course identifier (ObjectId converted to string)"),
            TableColumn(name="schoolId", type="string", description="Reference to school/organization"),
            TableColumn(name="code", type="string", description="Course code (e.g., 'ALG-1-2025')"),
            TableColumn(name="name", type="string", description="Course title"),
            TableColumn(name="subject", type="string", description="Subject area: 'math', 'language', 'science', etc."),
            TableColumn(name="gradeBand", type="string", description="Grade band: 'K-2', '3-5', '6-8', '9-12', etc."),
            TableColumn(name="academicYear", type="string", description="Academic year identifier (e.g., '2025-2026')"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_enrollments": {
        "description": "Student course enrollment records from MongoDB LMS. Links learners to courses with progress tracking. Aligned with Ed-Fi StudentSectionAssociation and Section entities.",
        "why": "Source for dim_section (grouped by sectionId), bridge_section_membership (memberships with roles), and dim_academic_term (distinct terms). Maps to Ed-Fi enrollment concepts.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique enrollment identifier (MongoDB ObjectId converted to string). Maps to Ed-Fi StudentSectionAssociation.UniqueId."),
            TableColumn(name="student", type="string", description="Reference to student user (ObjectId converted to string). Maps to Ed-Fi StudentSectionAssociation.StudentReference."),
            TableColumn(name="course", type="string", description="Reference to course (ObjectId converted to string). Maps to Ed-Fi Section.CourseOfferingReference."),
            TableColumn(name="group", type="string", description="Reference to group (ObjectId converted to string, can be null). For multi-group enrollments."),
            TableColumn(name="module", type="string", description="Reference to current module (ObjectId converted to string). Tracks learner's position in course."),
            TableColumn(name="status", type="string", description="Enrollment status: 'active', 'completed', 'dropped', etc. Maps to Ed-Fi StudentSectionAssociation.EndDate logic."),
            TableColumn(name="total_lessons", type="integer", description="Total lessons in the enrolled course. For progress calculation."),
            TableColumn(name="total_exercises", type="integer", description="Total exercises in the enrolled course. For progress calculation."),
            TableColumn(name="total_subjects", type="integer", description="Total subjects covered in the enrolled course."),
            TableColumn(name="total_progress", type="integer", description="Overall progress percentage in the course (0-100)."),
            TableColumn(name="current_lesson", type="string", description="Reference to current lesson (ObjectId converted to string, can be null). Tracks learner's current position."),
            TableColumn(name="summary", type="string", description="JSON array summarizing lesson progress, scores, coins, pages, history, skipped items, sections. Complex nested structure for detailed progress tracking."),
            TableColumn(name="criteria", type="string", description="JSON array of completion criteria. Defines requirements for course completion."),
            TableColumn(name="year", type="integer", description="Academic year of enrollment (e.g., 2025). Maps to Ed-Fi AcademicYear."),
            TableColumn(name="total_score", type="integer", description="Total score achieved in the course across all assessments."),
            TableColumn(name="published_score", type="integer", description="Published/visible score (may differ from total_score for privacy/grading policies)."),
            TableColumn(name="assistant", type="string", description="Reference to assistant user (ObjectId converted to string, optional). For AI-assisted learning."),
            TableColumn(name="mentor", type="string", description="Reference to mentor user (ObjectId converted to string, optional). For mentorship programs."),
            TableColumn(name="created_on", type="timestamp", description="Record creation time. Maps to Ed-Fi StudentSectionAssociation.BeginDate."),
            TableColumn(name="modified_on", type="timestamp", description="Record last update time. Used for incremental loading. Maps to Ed-Fi StudentSectionAssociation.LastModifiedDate."),
            TableColumn(name="__v", type="integer", description="Mongoose version key for optimistic concurrency control."),
        ]
    },
    "lms_pages": {
        "description": "Page content items from MongoDB LMS. One of three content types (pages, blocks, lessons) unified into dim_content.",
        "why": "Source for dim_content (union with blocks and lessons). Represents page-level content objects.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique content identifier (ObjectId converted to string)"),
            TableColumn(name="schoolId", type="string", description="Reference to school/organization"),
            TableColumn(name="courseId", type="string", description="Reference to course"),
            TableColumn(name="sectionId", type="string", description="Reference to section (optional)"),
            TableColumn(name="title", type="string", description="Content display title"),
            TableColumn(name="description", type="string", description="Content description"),
            TableColumn(name="interactionType", type="string", description="Interaction model: 'choice', 'numeric', 'drag-and-drop', etc."),
            TableColumn(name="difficultyEstimate", type="float", description="Estimated difficulty level"),
            TableColumn(name="primarySkillId", type="string", description="Primary skill this item targets"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="JSON: skill mappings, media, config"),
        ]
    },
    "lms_blocks": {
        "description": "Content block definitions from MongoDB LMS. One of three content types unified into dim_content.",
        "why": "Source for dim_content (union with pages and lessons). Represents block-level content objects.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique content identifier (ObjectId converted to string)"),
            TableColumn(name="schoolId", type="string", description="Reference to school/organization"),
            TableColumn(name="courseId", type="string", description="Reference to course"),
            TableColumn(name="sectionId", type="string", description="Reference to section (optional)"),
            TableColumn(name="title", type="string", description="Content display title"),
            TableColumn(name="description", type="string", description="Content description"),
            TableColumn(name="interactionType", type="string", description="Interaction model"),
            TableColumn(name="difficultyEstimate", type="float", description="Estimated difficulty level"),
            TableColumn(name="primarySkillId", type="string", description="Primary skill this item targets"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="JSON: skill mappings, media, config"),
        ]
    },
    "lms_lessons": {
        "description": "Lesson content and metadata from MongoDB LMS. One of three content types unified into dim_content.",
        "why": "Source for dim_content (union with pages and blocks). Represents lesson-level content objects.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique content identifier (ObjectId converted to string)"),
            TableColumn(name="schoolId", type="string", description="Reference to school/organization"),
            TableColumn(name="courseId", type="string", description="Reference to course"),
            TableColumn(name="sectionId", type="string", description="Reference to section (optional)"),
            TableColumn(name="title", type="string", description="Content display title"),
            TableColumn(name="description", type="string", description="Content description"),
            TableColumn(name="interactionType", type="string", description="Interaction model"),
            TableColumn(name="difficultyEstimate", type="float", description="Estimated difficulty level"),
            TableColumn(name="primarySkillId", type="string", description="Primary skill this item targets"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="JSON: skill mappings, media, config"),
        ]
    },
    "lms_exercises": {
        "description": "Exercise and activity definitions from MongoDB LMS. Used for both assessments (containers) and assessment items (questions). Aligned with Ed-Fi Assessment and AssessmentItem entities.",
        "why": "Source for dim_assessment (union with assessment_profiles) and dim_assessment_item (item-level breakdown). Maps to Ed-Fi Assessment/AssessmentItem concepts.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique exercise identifier (MongoDB ObjectId converted to string). Maps to Ed-Fi Assessment.AssessmentIdentifier or AssessmentItem.AssessmentItemIdentifier."),
            TableColumn(name="master", type="string", description="Reference to master exercise (ObjectId converted to string, optional). For exercise templates/variants."),
            TableColumn(name="title", type="string", description="Exercise title. Maps to Ed-Fi Assessment.AssessmentTitle or AssessmentItem.AssessmentItemDescription."),
            TableColumn(name="caption", type="string", description="Short caption for the exercise. Display label."),
            TableColumn(name="course", type="string", description="Reference to course (ObjectId converted to string). Maps to Ed-Fi Assessment.AssessmentCategory."),
            TableColumn(name="lesson", type="string", description="Reference to lesson (ObjectId converted to string). Context for where exercise appears."),
            TableColumn(name="questions", type="string", description="JSON array of question IDs (ObjectId references). Links to question bank items."),
            TableColumn(name="pages", type="string", description="JSON array of page references. For multi-page exercises."),
            TableColumn(name="subject", type="string", description="Subject area: 'math', 'language', 'science', etc. Maps to Ed-Fi Assessment.AcademicSubject."),
            TableColumn(name="complexity", type="string", description="Complexity level: 'simple', 'moderate', 'complex', etc."),
            TableColumn(name="difficulty", type="string", description="Difficulty level: 'easy', 'medium', 'hard', etc. Maps to Ed-Fi AssessmentItem.DifficultyLevel."),
            TableColumn(name="skills", type="string", description="JSON array of skill IDs. Skills this exercise targets. Maps to Ed-Fi Assessment.AssessedGradeLevel."),
            TableColumn(name="completion", type="string", description="JSON object: {gradable: boolean, attempts: number}. Completion criteria and grading settings."),
            TableColumn(name="settings", type="string", description="JSON object: {question_set: array, needs_review: boolean}. Assessment settings and question selection."),
            TableColumn(name="position", type="integer", description="Position/order within lesson or course. For sequencing."),
            TableColumn(name="created_on", type="timestamp", description="Record creation time. Maps to Ed-Fi Assessment.CreateDate."),
            TableColumn(name="modified_on", type="timestamp", description="Record last update time. Used for incremental loading. Maps to Ed-Fi Assessment.LastModifiedDate."),
            TableColumn(name="__v", type="integer", description="Mongoose version key for optimistic concurrency control."),
        ]
    },
    "lms_exercise_submissions": {
        "description": "Student exercise submissions and responses from MongoDB LMS. Contains both attempt-level and item-level data.",
        "why": "Source for fact_assessment_attempt (roll-up by submission) and fact_item_response (item-level breakdown).",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique submission identifier (ObjectId converted to string)"),
            TableColumn(name="userId", type="string", description="Reference to user/learner"),
            TableColumn(name="exerciseId", type="string", description="Reference to exercise/assessment"),
            TableColumn(name="sessionId", type="string", description="Reference to session"),
            TableColumn(name="schoolId", type="string", description="Reference to school"),
            TableColumn(name="courseId", type="string", description="Reference to course"),
            TableColumn(name="sectionId", type="string", description="Reference to section"),
            TableColumn(name="startedAt", type="timestamp", description="When attempt started"),
            TableColumn(name="submittedAt", type="timestamp", description="When attempt ended"),
            TableColumn(name="scoreRaw", type="float", description="Raw score"),
            TableColumn(name="scoreScaled", type="float", description="Score normalized to [0,1]"),
            TableColumn(name="scorePercent", type="float", description="Percentage score (0-100)"),
            TableColumn(name="passed", type="boolean", description="Whether attempt passed"),
            TableColumn(name="attemptNumber", type="int", description="Attempt count for this learner/exercise"),
            TableColumn(name="itemCount", type="int", description="Number of items in assessment"),
            TableColumn(name="completedItemCount", type="int", description="Number of items with valid responses"),
            TableColumn(name="itemResponses", type="string", description="JSON array of item-level responses"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_assessment_profiles": {
        "description": "Assessment profile configurations from MongoDB LMS. Defines assessment containers and structures.",
        "why": "Source for dim_assessment (union with exercises). Defines assessment types, scoring, and policies.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique assessment identifier (ObjectId converted to string)"),
            TableColumn(name="schoolId", type="string", description="Reference to school"),
            TableColumn(name="courseId", type="string", description="Reference to course"),
            TableColumn(name="contentId", type="string", description="Reference to content item (optional)"),
            TableColumn(name="title", type="string", description="Assessment title"),
            TableColumn(name="description", type="string", description="Assessment description"),
            TableColumn(name="assessmentType", type="string", description="Type: 'quiz', 'exam', 'placement', 'formative', 'summative', etc."),
            TableColumn(name="maxScore", type="float", description="Maximum score"),
            TableColumn(name="passingScore", type="float", description="Passing score threshold"),
            TableColumn(name="items", type="string", description="JSON array of assessment items with positions and points"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="JSON: timing, policies, question-set"),
        ]
    },
    "lms_events": {
        "description": "Learning events and interaction logs from MongoDB LMS (Caliper/xAPI style). Primary source for event facts and derived dimensions.",
        "why": "Multi-output source: fact_learning_event, dim_platform, dim_lti_tool, dim_ai_tool, dim_device, ref_event_type, ref_verb, ref_signal_type, fact_learning_signal, fact_variant_exposure, fact_ai_interaction, fact_launch, fact_session_summary, dim_date, dim_time_of_day.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique event identifier (ObjectId converted to string)"),
            TableColumn(name="eventTimestamp", type="timestamp", description="Event timestamp (UTC)"),
            TableColumn(name="eventType", type="string", description="Event type category"),
            TableColumn(name="verbId", type="int", description="xAPI verb identifier"),
            TableColumn(name="verbIri", type="string", description="Verb IRI from xAPI vocabulary"),
            TableColumn(name="verbShortCode", type="string", description="Short code for verb"),
            TableColumn(name="userId", type="string", description="Reference to user/learner"),
            TableColumn(name="teacherId", type="string", description="Reference to teacher (optional)"),
            TableColumn(name="sessionId", type="string", description="Reference to session"),
            TableColumn(name="schoolId", type="string", description="Reference to school"),
            TableColumn(name="courseId", type="string", description="Reference to course"),
            TableColumn(name="sectionId", type="string", description="Reference to section"),
            TableColumn(name="contentId", type="string", description="Reference to content item"),
            TableColumn(name="assessmentId", type="string", description="Reference to assessment"),
            TableColumn(name="skillId", type="string", description="Reference to skill"),
            TableColumn(name="platformId", type="string", description="Reference to platform"),
            TableColumn(name="platformName", type="string", description="Platform name"),
            TableColumn(name="vendor", type="string", description="Platform vendor"),
            TableColumn(name="version", type="string", description="Platform version"),
            TableColumn(name="environment", type="string", description="Environment: 'dev', 'stg', 'prod'"),
            TableColumn(name="ltiToolId", type="string", description="Reference to LTI tool"),
            TableColumn(name="ltiLaunchId", type="string", description="Reference to LTI launch"),
            TableColumn(name="issuer", type="string", description="LTI issuer"),
            TableColumn(name="clientId", type="string", description="LTI client_id"),
            TableColumn(name="toolName", type="string", description="LTI tool name"),
            TableColumn(name="deploymentId", type="string", description="LTI deployment ID"),
            TableColumn(name="aiToolId", type="string", description="Reference to AI tool"),
            TableColumn(name="aiInteractionId", type="string", description="Reference to AI interaction"),
            TableColumn(name="interactionRole", type="string", description="'user', 'system', 'assistant'"),
            TableColumn(name="inputTokens", type="int64", description="Token count for prompt"),
            TableColumn(name="outputTokens", type="int64", description="Token count for response"),
            TableColumn(name="latencyMs", type="int64", description="Response latency in milliseconds"),
            TableColumn(name="feedbackRating", type="float", description="AI response rating"),
            TableColumn(name="feedbackLabel", type="string", description="Feedback category"),
            TableColumn(name="transcriptSnippet", type="string", description="Interaction snippet"),
            TableColumn(name="isCorrect", type="boolean", description="Whether answer was correct"),
            TableColumn(name="scoreRaw", type="float", description="Raw score"),
            TableColumn(name="scoreScaled", type="float", description="Score normalized to [0,1]"),
            TableColumn(name="response", type="string", description="Learner response text"),
            TableColumn(name="attemptNumber", type="int", description="Attempt count"),
            TableColumn(name="eventSource", type="string", description="JSON: Raw event envelope"),
            TableColumn(name="signalId", type="string", description="Reference to learning signal"),
            TableColumn(name="signalTypeKey", type="string", description="Signal type key"),
            TableColumn(name="signalValue", type="float", description="Signal numeric value"),
            TableColumn(name="variantExposureId", type="string", description="Reference to variant exposure"),
            TableColumn(name="experimentKey", type="string", description="Experiment identifier"),
            TableColumn(name="variantKey", type="string", description="Variant identifier"),
            TableColumn(name="isControl", type="boolean", description="Whether control group"),
            TableColumn(name="activeSeconds", type="int", description="Active engagement time"),
            TableColumn(name="idleSeconds", type="int", description="Idle time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_login": {
        "description": "Login and session records from MongoDB LMS. Tracks user sessions with device and platform context.",
        "why": "Source for dim_session. Provides session boundaries, device information, and registration IDs.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique login record identifier (ObjectId converted to string)"),
            TableColumn(name="sessionId", type="string", description="Session identifier"),
            TableColumn(name="userId", type="string", description="Reference to user"),
            TableColumn(name="schoolId", type="string", description="Reference to school"),
            TableColumn(name="courseId", type="string", description="Reference to course"),
            TableColumn(name="sectionId", type="string", description="Reference to section"),
            TableColumn(name="platformId", type="string", description="Reference to platform"),
            TableColumn(name="deviceType", type="string", description="Device category: 'desktop', 'tablet', 'phone'"),
            TableColumn(name="userAgent", type="string", description="User agent string"),
            TableColumn(name="ipHash", type="string", description="Hashed IP address"),
            TableColumn(name="loginTime", type="timestamp", description="Session start time"),
            TableColumn(name="logoutTime", type="timestamp", description="Session end time"),
            TableColumn(name="durationSeconds", type="int64", description="Session duration"),
            TableColumn(name="registrationId", type="string", description="xAPI/cmi5 registration ID"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_rules": {
        "description": "Business rules and system configurations from MongoDB LMS. Contains skills and standards definitions.",
        "why": "Source for dim_skill (filter type='skill') and dim_standard (filter type='standard'). Defines competency frameworks.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique rule identifier (ObjectId converted to string)"),
            TableColumn(name="type", type="string", description="Rule type: 'skill', 'standard', 'config', etc."),
            TableColumn(name="externalCode", type="string", description="External skill/standard code"),
            TableColumn(name="name", type="string", description="Skill/rule name"),
            TableColumn(name="description", type="string", description="Description"),
            TableColumn(name="domain", type="string", description="Domain (e.g., 'Algebra', 'Reading Comprehension')"),
            TableColumn(name="strand", type="string", description="Subdomain or strand"),
            TableColumn(name="level", type="string", description="Difficulty or level"),
            TableColumn(name="parentSkillId", type="string", description="Parent skill in hierarchy"),
            TableColumn(name="parentStandardId", type="string", description="Parent standard in hierarchy"),
            TableColumn(name="framework", type="string", description="Framework (e.g., 'CCSS', 'local curriculum')"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_modules": {
        "description": "Course module structures from MongoDB LMS. Defines module hierarchies within courses.",
        "why": "May be used for content organization and module-level analytics. Currently not directly mapped to silver.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique module identifier (ObjectId converted to string)"),
            TableColumn(name="schoolId", type="string", description="Reference to school/organization"),
            TableColumn(name="courseId", type="string", description="Reference to course"),
            TableColumn(name="title", type="string", description="Module title"),
            TableColumn(name="description", type="string", description="Module description"),
            TableColumn(name="order", type="int", description="Module order/sequence"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_projects": {
        "description": "Project definitions and configurations from MongoDB LMS.",
        "why": "May be used for project-based learning analytics. Currently not directly mapped to silver.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique project identifier (ObjectId converted to string)"),
            TableColumn(name="schoolId", type="string", description="Reference to school/organization"),
            TableColumn(name="courseId", type="string", description="Reference to course"),
            TableColumn(name="title", type="string", description="Project title"),
            TableColumn(name="description", type="string", description="Project description"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_project_enrollments": {
        "description": "Project enrollment records from MongoDB LMS.",
        "why": "May be used for project participation analytics. Currently not directly mapped to silver.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique enrollment identifier (ObjectId converted to string)"),
            TableColumn(name="userId", type="string", description="Reference to user"),
            TableColumn(name="projectId", type="string", description="Reference to project"),
            TableColumn(name="status", type="string", description="Enrollment status"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_surveys": {
        "description": "Survey definitions from MongoDB LMS.",
        "why": "May be used for survey response analytics. Currently not directly mapped to silver.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique survey identifier (ObjectId converted to string)"),
            TableColumn(name="schoolId", type="string", description="Reference to school/organization"),
            TableColumn(name="title", type="string", description="Survey title"),
            TableColumn(name="description", type="string", description="Survey description"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_survey_enrollments": {
        "description": "Survey enrollment records from MongoDB LMS.",
        "why": "May be used for survey participation analytics. Currently not directly mapped to silver.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique enrollment identifier (ObjectId converted to string)"),
            TableColumn(name="userId", type="string", description="Reference to user"),
            TableColumn(name="surveyId", type="string", description="Reference to survey"),
            TableColumn(name="status", type="string", description="Enrollment status"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lms_survey_submissions": {
        "description": "Survey response submissions from MongoDB LMS.",
        "why": "May be used for survey response analytics. Currently not directly mapped to silver.",
        "fields": [
            TableColumn(name="_id", type="string", description="Unique submission identifier (ObjectId converted to string)"),
            TableColumn(name="userId", type="string", description="Reference to user"),
            TableColumn(name="surveyId", type="string", description="Reference to survey"),
            TableColumn(name="responses", type="string", description="JSON array of survey responses"),
            TableColumn(name="submittedAt", type="timestamp", description="Submission timestamp"),
            TableColumn(name="createdAt", type="timestamp", description="Record creation time"),
            TableColumn(name="updatedAt", type="timestamp", description="Record last update time"),
            TableColumn(name="metadata", type="string", description="Additional JSON metadata"),
        ]
    },
    "lrs_statements": {
        "description": (
            "xAPI (Experience API) learning statements from the LRS endpoint. Standard xAPI 1.0.3 format for learning activity records. "
            "Stored as JSON columns in BigQuery to preserve nested structure. Aligned with xAPI specification and Caliper Analytics events. "
            "Primary key: id (statement.id). Used for incremental loading via 'stored' timestamp."
        ),
        "why": (
            "Source for fact_learning_event (union with lms_events) and ref_verb (distinct verbs). Provides xAPI-standard learning records "
            "following IMS Global xAPI specification. Enables interoperability with xAPI-compliant systems. Stored as JSON to preserve "
            "full statement structure for staging layer extraction."
        ),
        "fields": [
            TableColumn(name="id", type="string", description="Unique statement identifier (xAPI statement.id, UUID format). Required by xAPI spec. Primary key. Maps to fact_learning_event.source_event_key."),
            TableColumn(name="timestamp", type="timestamp", description="Statement timestamp (xAPI statement.timestamp). When the event occurred. Maps to fact_learning_event.event_timestamp_utc."),
            TableColumn(name="stored", type="timestamp", description="Timestamp when LRS received the statement (xAPI statement.stored). Used for incremental loading. Enables CDC from LRS."),
            TableColumn(name="actor", type="string", description="JSON: Actor object (xAPI statement.actor). Contains {objectType: 'Agent', account: {name, homepage}, mbox, etc.}. Extracted in staging to userId. Maps to fact_learning_event.person_id."),
            TableColumn(name="verb", type="string", description="JSON: Verb object (xAPI statement.verb). Contains {id: IRI, display: {en-US: 'verb name'}}. Extracted in staging to verbId, verbIri, verbDescription. Maps to ref_verb and fact_learning_event.verb_id."),
            TableColumn(name="object", type="string", description="JSON: Object/Activity (xAPI statement.object). Contains {id: IRI, objectType: 'Activity', definition: {...}}. Extracted in staging to objectId, objectType. Maps to fact_learning_event.object_id."),
            TableColumn(name="result", type="string", description="JSON: Result object (xAPI statement.result). Contains {success: boolean, completion: boolean, score: {raw, min, max, scaled}, response: string, duration: string}. Extracted in staging to success, completion, scoreRaw, etc. Maps to assessment attempt facts."),
            TableColumn(name="context", type="string", description="JSON: Context object (xAPI statement.context). Contains {registration: UUID, instructor: Agent, team: Group, contextActivities: {...}, extensions: {...}}. Includes course_id, attempts, time_taken_ms in extensions. Extracted in staging."),
            TableColumn(name="authority", type="string", description="JSON: Authority object (xAPI statement.authority). Contains {objectType: 'Agent', account: {...}}. Who asserted this statement. For audit and trust."),
            TableColumn(name="version", type="string", description="xAPI version used (e.g., '1.0.3'). From X-Experience-API-Version header. Ensures compatibility."),
            TableColumn(name="attachments", type="string", description="JSON array: Attachments (xAPI statement.attachments). Optional file attachments to the statement. Rarely used."),
        ]
    },
    "xapi_statements": {
        "description": "Alias for lrs_statements. xAPI learning statements from the LRS endpoint.",
        "why": "Alias table name for compatibility. Maps to lrs_statements.",
        "fields": []  # Same as lrs_statements
    },
}


def get_raw_table_metadata(collection_name: str) -> dict:
    """Get metadata for a raw table including schema, description, and why."""
    schema_info = RAW_TABLE_SCHEMAS.get(collection_name, {})
    
    if not schema_info:
        return {
            "description": MetadataValue.text(f"Raw data from MongoDB LMS `{collection_name}` collection"),
            "source": MetadataValue.text("MongoDB LMS"),
            "collection": MetadataValue.text(collection_name),
        }
    
    metadata = {
        "description": MetadataValue.text(schema_info.get("description", "")),
        "why": MetadataValue.text(schema_info.get("why", "")),
        "source": MetadataValue.text("MongoDB LMS" if collection_name.startswith("lms_") else "xAPI LRS"),
        "collection": MetadataValue.text(collection_name),
    }
    
    # Add table schema if fields are defined
    if schema_info.get("fields"):
        schema = TableSchema(columns=schema_info["fields"])
        metadata["schema"] = MetadataValue.table_schema(schema)
        metadata["field_count"] = MetadataValue.int(len(schema_info["fields"]))
    
    return metadata

