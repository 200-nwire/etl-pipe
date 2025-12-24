"""Schema definitions for staging tables with field metadata for Dagster UI.

Staging layer prepares raw data for silver transformation by:
- Extracting JSON fields into structured columns
- Standardizing field names and types
- Adding computed fields for downstream use
- Aligning with Ed-Fi, Caliper, and xAPI standards
"""

from dagster import MetadataValue, TableColumn, TableSchema

# Staging table schemas
STAGING_TABLE_SCHEMAS = {
    "stg_lrs_events": {
        "description": (
            "Staging model to prepare xAPI statements for silver layer transformation. "
            "Maps xAPI statement JSON fields to a common event structure compatible with "
            "Caliper Analytics and Ed-Fi event models. Extracts nested JSON using BigQuery JSON functions."
        ),
        "why": (
            "Prepares raw xAPI statements (stored as JSON in BigQuery) for transformation into "
            "fact_learning_event and other silver facts. Standardizes xAPI actor/verb/object/result/context "
            "structure into flat columns for easier SQL transformation. Aligns xAPI data with "
            "lms_events structure for unified fact_learning_event."
        ),
        "fields": [
            TableColumn(name="event_id", type="string", description="Event identifier extracted from xAPI statement.id. Maps to fact_learning_event.source_event_key."),
            TableColumn(name="event_timestamp", type="timestamp", description="Event timestamp from xAPI statement.timestamp. Maps to fact_learning_event.event_timestamp_utc."),
            TableColumn(name="userId", type="string", description="Actor identifier extracted from xAPI statement.actor.account.name. Maps to fact_learning_event.person_id."),
            TableColumn(name="verbId", type="string", description="Verb identifier extracted from xAPI statement.verb.id (IRI). Maps to ref_verb.iri."),
            TableColumn(name="verbIri", type="string", description="Verb IRI from xAPI statement.verb.id. Full IRI format (e.g., 'http://adlnet.gov/expapi/verbs/completed')."),
            TableColumn(name="verbShortCode", type="string", description="Verb short code (null in staging, populated in silver). Normalized verb code."),
            TableColumn(name="verbDescription", type="string", description="Verb description from xAPI statement.verb.display.en-US. Human-readable verb name."),
            TableColumn(name="objectId", type="string", description="Object identifier from xAPI statement.object.id. Maps to fact_learning_event.object_id."),
            TableColumn(name="objectType", type="string", description="Object type from xAPI statement.object.objectType. Maps to ref_object_type.object_type_key."),
            TableColumn(name="sessionId", type="string", description="Session identifier (null in staging, may be extracted from context). Maps to dim_session.session_id."),
            TableColumn(name="schoolId", type="string", description="School/organization identifier (null in staging, may be extracted from context). Maps to dim_organization.organization_id."),
            TableColumn(name="courseId", type="string", description="Course identifier extracted from xAPI context.extensions. Maps to dim_course.course_id."),
            TableColumn(name="sectionId", type="string", description="Section identifier (null in staging, may be extracted from context). Maps to dim_section.section_id."),
            TableColumn(name="assessmentId", type="string", description="Assessment identifier (null in staging, may be extracted from context). Maps to dim_assessment.assessment_id."),
            TableColumn(name="skillId", type="string", description="Skill identifier (null in staging, may be extracted from context). Maps to dim_skill.skill_id."),
            TableColumn(name="platformId", type="string", description="Platform identifier (null in staging, may be extracted from context). Maps to dim_platform.platform_id."),
            TableColumn(name="ltiToolId", type="string", description="LTI tool identifier (null in staging, may be extracted from context). Maps to dim_lti_tool.lti_tool_id."),
            TableColumn(name="aiToolId", type="string", description="AI tool identifier (null in staging, may be extracted from context). Maps to dim_ai_tool.ai_tool_id."),
            TableColumn(name="deviceId", type="string", description="Device identifier (null in staging, may be extracted from context). Maps to dim_device.device_id."),
            TableColumn(name="deviceType", type="string", description="Device type (null in staging, may be extracted from context). Maps to dim_device.device_type."),
            TableColumn(name="os", type="string", description="Operating system (null in staging, may be extracted from context)."),
            TableColumn(name="browser", type="string", description="Browser type (null in staging, may be extracted from context)."),
            TableColumn(name="userAgent", type="string", description="User agent string (null in staging, may be extracted from context)."),
            TableColumn(name="termId", type="string", description="Term identifier (null in staging, may be extracted from context). Maps to dim_academic_term.term_id."),
            TableColumn(name="membershipId", type="string", description="Membership identifier (null in staging, may be extracted from context). Maps to bridge_section_membership.section_membership_id."),
            TableColumn(name="targetType", type="string", description="Target type (null in staging, may be extracted from context). Caliper concept."),
            TableColumn(name="targetId", type="string", description="Target identifier (null in staging, may be extracted from context). Caliper concept."),
            TableColumn(name="generatedType", type="string", description="Generated object type (null in staging, may be extracted from context). Caliper concept."),
            TableColumn(name="generatedId", type="string", description="Generated object identifier (null in staging, may be extracted from context). Caliper concept."),
            TableColumn(name="contentId", type="string", description="Content identifier extracted from xAPI statement.object.id. Maps to dim_content.content_id."),
            TableColumn(name="itemId", type="string", description="Item identifier (null in staging, may be extracted from context). Maps to dim_assessment_item.item_id."),
            TableColumn(name="standardId", type="string", description="Standard identifier (null in staging, may be extracted from context). Maps to dim_standard.standard_id."),
            TableColumn(name="attemptNumber", type="int", description="Attempt number extracted from xAPI context.extensions. Maps to fact_assessment_attempt and fact_item_response."),
            TableColumn(name="success", type="boolean", description="Success flag extracted from xAPI statement.result.success. Maps to fact_assessment_attempt and fact_item_response."),
            TableColumn(name="completion", type="boolean", description="Completion flag extracted from xAPI statement.result.completion. Maps to fact_learning_event."),
            TableColumn(name="isCorrect", type="boolean", description="Correctness flag derived from xAPI statement.result.success. Maps to fact_item_response."),
            TableColumn(name="scoreRaw", type="float", description="Raw score extracted from xAPI statement.result.score.raw. Maps to fact_assessment_attempt and fact_item_response."),
            TableColumn(name="scoreMin", type="float", description="Minimum score from xAPI statement.result.score.min. For score normalization."),
            TableColumn(name="scoreMax", type="float", description="Maximum score from xAPI statement.result.score.max. For score normalization."),
            TableColumn(name="scoreScaled", type="float", description="Scaled score from xAPI statement.result.score.scaled (0-1 range). Maps to fact_assessment_attempt."),
            TableColumn(name="scorePercent", type="float", description="Percentage score (null in staging, computed in silver). Derived from scoreRaw/scoreMax."),
            TableColumn(name="response", type="string", description="Learner response extracted from xAPI statement.result.response. Maps to fact_item_response.response."),
            TableColumn(name="durationSeconds", type="int", description="Duration in seconds extracted from xAPI context.extensions.time_taken_ms. Maps to fact_learning_event.duration_seconds."),
            TableColumn(name="latencyMs", type="int", description="Latency in milliseconds (null in staging, for AI interactions)."),
            TableColumn(name="eventType", type="string", description="Event type derived from xAPI statement.verb.id. Maps to ref_event_type.event_type_key."),
            TableColumn(name="eventSource", type="string", description="Event source (null in staging, populated in silver). Source system identifier."),
            TableColumn(name="timezone", type="string", description="Timezone (null in staging, may be extracted from context). For local time conversion."),
            TableColumn(name="actorType", type="string", description="Actor type: 'learner' (default for xAPI). Maps to fact_learning_event.actor_type."),
            TableColumn(name="impersonatedUserId", type="string", description="Impersonated user ID (null in staging, may be extracted from context). For acting-on-behalf-of scenarios."),
            TableColumn(name="toolName", type="string", description="Tool name (null in staging, may be extracted from context). For LTI/AI tools."),
            TableColumn(name="provider", type="string", description="Provider name (null in staging, may be extracted from context). For AI tools."),
            TableColumn(name="modelName", type="string", description="Model name (null in staging, may be extracted from context). For AI tools."),
            TableColumn(name="capabilityType", type="string", description="Capability type (null in staging, may be extracted from context). For AI tools."),
            TableColumn(name="platformName", type="string", description="Platform name (null in staging, may be extracted from context). Maps to dim_platform.name."),
            TableColumn(name="vendor", type="string", description="Vendor name (null in staging, may be extracted from context). Maps to dim_platform metadata."),
            TableColumn(name="version", type="string", description="Version (null in staging, may be extracted from context). Maps to dim_platform metadata."),
            TableColumn(name="environment", type="string", description="Environment (null in staging, may be extracted from context). Maps to dim_platform.environment."),
            TableColumn(name="launchType", type="string", description="Launch type (null in staging, may be extracted from context). For LTI launches."),
            TableColumn(name="ltiLaunchId", type="string", description="LTI launch ID (null in staging, may be extracted from context). Maps to fact_launch.launch_id."),
            TableColumn(name="variantId", type="string", description="Variant ID (null in staging, may be extracted from context). For A/B testing."),
            TableColumn(name="experimentId", type="string", description="Experiment ID (null in staging, may be extracted from context). For A/B testing."),
            TableColumn(name="signalType", type="string", description="Signal type (null in staging, may be extracted from context). Maps to ref_signal_type.signal_type_key."),
            TableColumn(name="signalValue", type="string", description="Signal value (null in staging, may be extracted from context). Maps to fact_learning_signal."),
            TableColumn(name="metadata", type="string", description="JSON: Full xAPI context object. Preserved for downstream extraction of additional fields."),
            TableColumn(name="fullStatement", type="string", description="JSON: Complete xAPI statement reconstructed from all fields. Preserved for audit and advanced analytics."),
        ]
    },
}


def get_staging_table_metadata(table_name: str) -> dict:
    """Get metadata for a staging table including schema, description, and why."""
    schema_info = STAGING_TABLE_SCHEMAS.get(table_name, {})
    
    if not schema_info:
        return {
            "description": MetadataValue.text(f"Staging table: {table_name}"),
            "source": MetadataValue.text("dbt transformation"),
            "layer": MetadataValue.text("staging"),
        }
    
    metadata = {
        "description": MetadataValue.text(schema_info.get("description", "")),
        "why": MetadataValue.text(schema_info.get("why", "")),
        "source": MetadataValue.text("dbt transformation"),
        "layer": MetadataValue.text("staging"),
    }
    
    # Add table schema if fields are defined
    if schema_info.get("fields"):
        schema = TableSchema(columns=schema_info["fields"])
        metadata["schema"] = MetadataValue.table_schema(schema)
        metadata["field_count"] = MetadataValue.int(len(schema_info["fields"]))
    
    return metadata


