"""Schema definitions for silver tables with field metadata for Dagster UI.

Silver layer follows dimensional modeling best practices and aligns with:
- Ed-Fi Data Model: Student, Staff, Section, Assessment, etc.
- Caliper Analytics 1.2: Event, Entity, MetricProfile
- xAPI 1.0.3: Statement, Actor, Verb, Object, Result, Context
"""

from dagster import MetadataValue, TableSchema, TableColumn

# Import complete schemas from generated file
try:
    from lineage.schemas.silver_schemas_complete import SILVER_TABLE_SCHEMAS
except ImportError:
    # Fallback if generated file doesn't exist yet
    SILVER_TABLE_SCHEMAS = {}

# Import staging schemas
try:
    from lineage.schemas.staging_schemas import get_staging_table_metadata
except ImportError:
    def get_staging_table_metadata(table_name: str) -> dict:
        return {
            "description": MetadataValue.text(f"Staging table: {table_name}"),
            "source": MetadataValue.text("dbt transformation"),
            "layer": MetadataValue.text("staging"),
        }


def get_silver_table_metadata(table_name: str, dbt_resource_props: dict = None) -> dict:
    """Get metadata for a silver table including schema, description, and why.
    
    Args:
        table_name: Name of the silver table (e.g., 'dim_person')
        dbt_resource_props: Optional dbt resource properties from manifest
    """
    schema_info = SILVER_TABLE_SCHEMAS.get(table_name, {})
    
    metadata = {
        "description": MetadataValue.text(
            schema_info.get("description") or 
            dbt_resource_props.get("description", "") if dbt_resource_props else 
            f"Silver layer table: {table_name}"
        ),
        "source": MetadataValue.text("dbt transformation"),
        "layer": MetadataValue.text("silver"),
    }
    
    # Add why/what information
    if schema_info.get("why"):
        metadata["why"] = MetadataValue.text(schema_info["why"])
    
    # Add table schema if fields are defined
    if schema_info.get("fields"):
        schema = TableSchema(columns=schema_info["fields"])
        metadata["schema"] = MetadataValue.table_schema(schema)
        metadata["field_count"] = MetadataValue.int(len(schema_info["fields"]))
    
    # Add lineage information if available from dbt
    if dbt_resource_props:
        depends_on = dbt_resource_props.get("depends_on", {})
        source_nodes = depends_on.get("nodes", [])
        raw_sources = []
        for node_id in source_nodes:
            if "source" in node_id:
                parts = node_id.split(".")
                if len(parts) >= 4:
                    source_name = parts[2]  # raw_mongo or raw_xapi
                    table_name_raw = parts[3]  # lms_users, etc.
                    raw_sources.append(f"{source_name}.{table_name_raw}")
        
        if raw_sources:
            metadata["raw_sources"] = MetadataValue.text(", ".join(raw_sources))
            metadata["lineage"] = MetadataValue.md(
                f"**Raw Sources**: {', '.join([f'`{s}`' for s in raw_sources])}"
            )
    
    return metadata

