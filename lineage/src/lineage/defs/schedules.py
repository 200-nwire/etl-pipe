"""Schedules for automatic data ingestion."""

from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# NOTE: raw_tables assets are now created by dlt component (dlt_loads/defs.yaml)
# Since dlt component assets don't have group_name set and we can't easily select them
# by pattern at definition time, we'll create a simple job that selects all assets
# Users can manually select raw assets in the UI, or we can add tags to dlt component assets later

# For now, create a job that selects all assets - users can filter in UI
# TODO: Add tags to dlt component assets so we can select by tag
raw_ingestion_job = define_asset_job(
    name="raw_ingestion_job",
    selection=AssetSelection.all(),  # Select all assets - user can filter in UI
    description=(
        "Ingest all raw data from MongoDB LMS and xAPI LRS via dlt component. "
        "Filter to raw assets in UI."
    ),
)

# Schedule to run every 15 minutes (disabled by default until we can properly select raw assets)
raw_ingestion_schedule = ScheduleDefinition(
    name="raw_ingestion_schedule",
    job=raw_ingestion_job,
    cron_schedule="*/15 * * * *",  # Every 15 minutes
    # Disabled until proper asset selection is configured
    default_status=DefaultScheduleStatus.STOPPED,
    description="Automatically ingest raw data from sources every 15 minutes",
)
