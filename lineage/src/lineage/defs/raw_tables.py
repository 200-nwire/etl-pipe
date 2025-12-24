"""Individual raw table assets for each ingested collection/table.

Each MongoDB collection gets its own asset for independent materialization and visualization.
"""

import os

from dagster import (
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    Output,
    asset,
)

from lineage.resources import BigQueryConfig, LRSConfig, MongoDBConfig
from lineage.schemas.raw_schemas import get_raw_table_metadata

# Import from sources module
from lineage.sources.lms import COLLECTION_TO_TABLE_MAP, DEFAULT_MONGO_COLLECTIONS, load_mongo_raw
from lineage.sources.lrs import load_xapi_raw

COLLECTION_DESCRIPTIONS = {
    "users": "User accounts, profiles, and authentication data",
    "schools": "School and organization records",
    "grades": "Grade level definitions and configurations",
    "disciplines": "Subject/discipline categories",
    "skills": "Skill definitions and competency taxonomies",
    "stages": "Learning stage definitions",
    "personas": "User persona configurations",
    "courses": "Course catalog and course definitions",
    "modules": "Course module structures",
    "lessons": "Lesson content and metadata",
    "pages": "Page content and structure",
    "blocks": "Content block definitions",
    "exercises": "Exercise and activity definitions",
    "exercise_submissions": "Student exercise submissions and responses",
    "enrollments": "Student course enrollment records",
    "projects": "Project definitions and configurations",
    "project_enrollments": "Project enrollment records",
    "surveys": "Survey definitions",
    "survey_enrollments": "Survey enrollment records",
    "survey_submissions": "Survey response submissions",
    "questionnaires": "Questionnaire definitions",
    "assessment_profiles": "Assessment profile configurations",
    "events": "Learning events and interaction logs",
    "notifications": "Notification records",
    "login": "Login and session records",
    "rules": "Business rules and system configurations",
    "config": "System configuration settings",
    "versions": "Version tracking and history",
    "meeting": "Meeting and collaboration records",
}


def _create_mongo_raw_asset(collection: str):
    """Create an individual asset for a MongoDB collection.
    
    Each collection gets its own asset for independent materialization and visualization.
    """
    # Map collection name to table name (e.g., "submissions" -> "exercise_submissions")
    # Also convert hyphens to underscores for SQL compatibility
    mapped_name = COLLECTION_TO_TABLE_MAP.get(collection, collection)
    # Convert any remaining hyphens to underscores
    # (e.g., "project-enrollments" -> "project_enrollments")
    mapped_name = mapped_name.replace("-", "_")
    table_name = f"lms_{mapped_name}"
    
    @asset(
        key=AssetKey(["raw", table_name]),
        group_name="raw",
        deps=[AssetKey("source_mongodb_lms")],  # Connect to MongoDB source
        description=COLLECTION_DESCRIPTIONS.get(
            collection, 
            f"Raw data from MongoDB LMS `{collection}` collection"
        ),
        compute_kind="dlt",
        metadata=get_raw_table_metadata(table_name),
        # Auto-materialize downstream dbt assets when this raw table updates
        # This is handled by the dbt assets' auto_materialize_policy
    )
    def mongo_collection_asset(
        context: AssetExecutionContext,
        bq_config: BigQueryConfig,
        mongo_config: MongoDBConfig,
    ) -> Output[str]:
        """Ingest a single MongoDB collection into raw table with lms_ prefix.
        
        Uses configurable resources for BigQuery and MongoDB settings.
        These can be configured from the Dagster UI per-run.
        """
        # Load only this specific collection
        # Pass configurable resources to load function
        load_mongo_raw(
            collections=[collection],
            bq_config=bq_config,
            mongo_config=mongo_config,
        )
        destination = os.environ.get("DLT_DESTINATION", "bigquery")
        desc = COLLECTION_DESCRIPTIONS.get(collection, f"Raw data from {collection} collection")
        
        return Output(
            value=collection,
            metadata={
                "table": MetadataValue.text(f"raw.{table_name}"),
                "source_collection": MetadataValue.text(collection),
                "destination": MetadataValue.text(destination),
                "lineage": MetadataValue.md(
                    f"**Source**: MongoDB LMS → `{collection}` collection\n\n"
                    f"**Destination**: `raw.{table_name}` table in {destination}\n\n"
                    f"**Description**: {desc}\n\n"
                    "Raw data ingested via dlt pipeline, ready for "
                    "transformation in staging/silver layers."
                ),
            },
        )
    
    return mongo_collection_asset


# Create individual assets for each LMS collection
# This allows independent materialization and visualization per table
lms_raw_assets = [
    _create_mongo_raw_asset(collection) 
    for collection in DEFAULT_MONGO_COLLECTIONS
]


# LRS raw table asset
@asset(
    group_name="raw",
    key=AssetKey(["raw", "lrs", "lrs_statements"]),
    deps=[AssetKey("source_xapi_lrs")],  # Connect to LRS source
    description=(
        "**Raw LRS Statements Table**\n\n"
        "Learning Record Store statements containing:\n"
        "- Learning activity tracking\n"
        "- Assessment attempts and results\n"
        "- Content engagement metrics\n"
        "- Learning signals and analytics events\n\n"
        "Ingested from LRS via REST API into the raw schema."
    ),
    compute_kind="dbt",
    metadata=get_raw_table_metadata("xapi_statements"),
    # Resources bq_config and lrs_config are injected automatically
)
def lrs_raw_table_asset(
    context: AssetExecutionContext,
    bq_config: BigQueryConfig,
    lrs_config: LRSConfig,
) -> Output[str]:
    """Raw LRS statements table asset.
    
    Uses configurable resources for BigQuery and LRS settings.
    These can be configured from the Dagster UI per-run.
    """
    # Pass configurable resources to load function
    load_xapi_raw(
        bq_config=bq_config,
        lrs_config=lrs_config,
    )
    destination = os.environ.get("DLT_DESTINATION", "bigquery")
    endpoint = (
        lrs_config.endpoint
        if lrs_config.endpoint
        else os.environ.get("XAPI_LRS_ENDPOINT", "Not configured")
    )
    
    return Output(
        value="lrs_statements",
        metadata={
            "table": MetadataValue.text("raw.lrs_statements"),
            "source": MetadataValue.text("LRS"),
            "destination": MetadataValue.text(destination),
            "endpoint": MetadataValue.text(endpoint),
            "lineage": MetadataValue.md(
                f"**Source**: LRS ({endpoint})\n\n"
                f"**Destination**: `raw.lrs_statements` table in {destination}\n\n"
                f"**Data Type**: xAPI Statements (JSON)\n\n"
                f"Raw learning statements ingested via dlt REST API loader, ready for "
                f"transformation into silver fact and dimension tables."
            ),
        },
    )

