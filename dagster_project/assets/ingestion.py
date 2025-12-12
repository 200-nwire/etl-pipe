"""Dagster assets that orchestrate dlt-based ingestion pipelines."""

from dagster import AssetExecutionContext, MetadataValue, Output, asset

from pipelines.mongo import load_mongo_raw
from pipelines.xapi import load_xapi_raw


@asset(
    name="mongo_raw_ingestion",
    group_name="ingestion",
    description=(
        "Run the MongoDB -> BigQuery raw ingestion via dlt. Results land in the raw dataset "
        "and are versioned with load packages."
    ),
    compute_kind="dlt",
    required_resource_keys={"secrets"},
)
def mongo_raw_asset(context: AssetExecutionContext) -> Output[str]:
    table_names = load_mongo_raw()
    return Output(
        value=" ,".join(table_names),
        metadata={
            "raw_tables": MetadataValue.json(table_names),
            "lineage": MetadataValue.md(
                "Ingests MongoDB collections exposed via Airbyte into raw BigQuery tables."
            ),
        },
    )


@asset(
    name="xapi_raw_ingestion",
    group_name="ingestion",
    description="Ingest xAPI LRS statements into the raw dataset using dlt's REST loader.",
    compute_kind="dlt",
    required_resource_keys={"secrets"},
)
def xapi_raw_asset(context: AssetExecutionContext) -> Output[str]:
    table_name = load_xapi_raw()
    return Output(
        value=table_name,
        metadata={
            "raw_table": MetadataValue.text(table_name),
            "lineage": MetadataValue.md(
                "Fetches paginated xAPI statements from the LRS and stores them in raw BigQuery."
            ),
        },
    )
