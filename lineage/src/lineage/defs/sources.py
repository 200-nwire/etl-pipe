"""Source system assets representing external data sources."""

from dagster import AssetExecutionContext, MetadataValue, Output, asset
import os


@asset(
    name="source_mongodb_lms",
    group_name="sources",
    description=(
        "**MongoDB LMS Source**\n\n"
        "Primary Learning Management System database containing:\n"
        "- User accounts and profiles\n"
        "- Course and content structures\n"
        "- Enrollment and assessment data\n"
        "- Learning events and interactions\n\n"
        "Connected via MongoDB Atlas cluster."
    ),
    compute_kind="external",
    metadata={
        "source_type": MetadataValue.text("MongoDB"),
        "system": MetadataValue.text("LMS"),
        "icon": MetadataValue.text("database"),
        "connection": MetadataValue.text(os.environ.get("MONGO_URI", "Not configured").split("@")[0] + "@..."),
    },
)
def mongodb_source_asset(context: AssetExecutionContext) -> Output[str]:
    """External source asset representing MongoDB LMS database."""
    mongo_uri = os.environ.get("MONGO_URI", "Not configured")
    return Output(
        value="mongodb_lms",
        metadata={
            "source": MetadataValue.text("MongoDB LMS"),
            "connection_string": MetadataValue.text(mongo_uri.split("@")[0] + "@..."),
            "lineage": MetadataValue.md(
                "**Source System**: MongoDB Learning Management System\n\n"
                "**Location**: MongoDB Atlas Cluster\n\n"
                "**Collections**: 29 collections including users, courses, enrollments, events, etc.\n\n"
                "This is the primary source of truth for learner data, course structures, "
                "and learning interactions in the LMS platform."
            ),
        },
    )


@asset(
    name="source_xapi_lrs",
    group_name="sources",
    description=(
        "**xAPI LRS Source**\n\n"
        "Learning Record Store containing xAPI statements that track:\n"
        "- Learning activities and interactions\n"
        "- Assessment attempts and results\n"
        "- Content engagement and completion\n"
        "- Learning signals and analytics events\n\n"
        "Connected via REST API with Basic Authentication."
    ),
    compute_kind="external",
    metadata={
        "source_type": MetadataValue.text("xAPI LRS"),
        "system": MetadataValue.text("LRS"),
        "icon": MetadataValue.text("api"),
        "endpoint": MetadataValue.text(os.environ.get("XAPI_LRS_ENDPOINT", "Not configured")),
    },
)
def xapi_source_asset(context: AssetExecutionContext) -> Output[str]:
    """External source asset representing xAPI Learning Record Store."""
    endpoint = os.environ.get("XAPI_LRS_ENDPOINT", "Not configured")
    return Output(
        value="xapi_lrs",
        metadata={
            "source": MetadataValue.text("xAPI LRS"),
            "endpoint": MetadataValue.text(endpoint),
            "lineage": MetadataValue.md(
                "**Source System**: xAPI Learning Record Store\n\n"
                "**Endpoint**: " + endpoint + "\n\n"
                "**Authentication**: Basic Auth\n\n"
                "**Data Type**: xAPI Statements (JSON)\n\n"
                "The LRS stores learning experiences in a standardized format, enabling "
                "interoperability across learning platforms and tools."
            ),
        },
    )


