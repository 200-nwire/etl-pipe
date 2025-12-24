"""Custom DbtProjectComponent that preserves custom translator logic.

This component creates individual assets per dbt model (not a single multi-asset),
so each model appears separately in the Dagster UI with its own dbt icon.
"""

import os
from pathlib import Path
from typing import Any, Optional
from collections.abc import Iterator, Mapping

from dagster import AssetExecutionContext, AssetKey, AssetSpec, MetadataValue, AutoMaterializePolicy, AutomationCondition
from dagster_dbt import DbtCliResource, DbtProject, DbtProjectComponent, DagsterDbtTranslator

from lineage.schemas.silver_schemas import get_silver_table_metadata
from lineage.schemas.staging_schemas import get_staging_table_metadata

# Import mapping from dbt_translator
from lineage.dbt_translator import DBT_SOURCE_TO_RAW_MAPPING


class CustomDbtProjectComponent(DbtProjectComponent):
    """Custom DbtProjectComponent that creates individual assets per dbt model.
    
    This replaces the @dbt_assets decorator approach, ensuring each dbt model
    appears as a separate asset in Dagster UI with its own dbt icon.
    """
    
    def get_asset_spec(
        self, manifest: Mapping[str, Any], unique_id: str, project: Optional[DbtProject]
    ) -> AssetSpec:
        """Get asset spec for a dbt resource, preserving custom translator logic.
        
        This method is called for each dbt model to create individual assets.
        """
        # Get base asset spec from parent
        asset_spec = super().get_asset_spec(manifest, unique_id, project)
        
        if not asset_spec:
            return asset_spec
        
        # Get the dbt resource properties
        nodes = manifest.get("nodes", {})
        sources = manifest.get("sources", {})
        resource_props = nodes.get(unique_id) or sources.get(unique_id)
        
        if not resource_props:
            return asset_spec
        
        resource_type = resource_props.get("resource_type")
        if resource_type != "model":
            return asset_spec
        
        # Extract dependencies from dbt model (same logic as CustomDagsterDbtTranslator)
        dependencies = []
        depends_on = resource_props.get("depends_on", {})
        source_nodes = depends_on.get("nodes", [])
        
        model_name = resource_props.get("name", "")
        is_staging = model_name.startswith("stg_") or "staging" in resource_props.get("schema", "").lower()
        
        for node_id in source_nodes:
            # Check if this is a reference to another dbt model (staging -> silver dependency)
            if "model" in node_id:
                parts = node_id.split(".")
                if len(parts) >= 3:
                    ref_model_name = parts[-1]
                    if not is_staging and ref_model_name.startswith("stg_"):
                        staging_key = AssetKey(["staging", ref_model_name])
                        if staging_key not in dependencies:
                            dependencies.append(staging_key)
                continue
            
            # Handle source dependencies (raw -> staging or raw -> silver)
            if "source" in node_id:
                parts = node_id.split(".")
                if len(parts) >= 4:
                    source_name = parts[2]
                    table_name_raw = parts[3]
                elif len(parts) >= 3:
                    source_name = parts[1]
                    table_name_raw = parts[2]
                else:
                    continue
                    
                if source_name == "raw_lms":
                    if table_name_raw.startswith("lms_"):
                        raw_key = AssetKey(["raw", table_name_raw])
                        if raw_key not in dependencies:
                            dependencies.append(raw_key)
                    else:
                        raw_collection = DBT_SOURCE_TO_RAW_MAPPING.get(table_name_raw, table_name_raw)
                        raw_key = AssetKey(["raw", f"lms_{raw_collection}"])
                        if raw_key not in dependencies:
                            dependencies.append(raw_key)
                elif source_name == "raw_lrs":
                    raw_key = AssetKey(["raw", "lrs", "lrs_statements"])
                    if raw_key not in dependencies:
                        dependencies.append(raw_key)
        
        # Get description from resource_props
        description = resource_props.get("description", asset_spec.description or "")
        
        # Build enhanced metadata with schema, lineage, and why/what info
        metadata = dict(asset_spec.metadata) if asset_spec.metadata else {}
        
        # Add comprehensive table metadata
        table_name = resource_props.get("name", "")
        if is_staging:
            staging_metadata = get_staging_table_metadata(table_name)
            metadata.update(staging_metadata)
        else:
            silver_metadata = get_silver_table_metadata(table_name, resource_props)
            metadata.update(silver_metadata)
        
        # Extract raw source dependencies and add to metadata
        raw_sources_list = []
        for node_id in source_nodes:
            if "source" in node_id:
                parts = node_id.split(".")
                if len(parts) >= 4:
                    source_name = parts[2]
                    table_name_raw = parts[3]
                elif len(parts) >= 3:
                    source_name = parts[1]
                    table_name_raw = parts[2]
                else:
                    continue
                    
                if source_name == "raw_lms":
                    if table_name_raw.startswith("lms_"):
                        raw_sources_list.append(f"raw.{table_name_raw}")
                    else:
                        raw_collection = DBT_SOURCE_TO_RAW_MAPPING.get(table_name_raw, table_name_raw)
                        raw_sources_list.append(f"raw.lms_{raw_collection}")
                elif source_name == "raw_lrs":
                    raw_sources_list.append(f"raw.lrs.{table_name_raw}")
        
        if raw_sources_list and "raw_sources" not in metadata:
            metadata["raw_sources"] = MetadataValue.text(", ".join(raw_sources_list))
            metadata["lineage"] = MetadataValue.md(
                f"**Raw Sources**: {', '.join([f'`{s}`' for s in raw_sources_list])}"
            )
        
        # Determine group name
        if is_staging:
            group_name = "staging"
            asset_key = AssetKey(["staging", model_name])
        else:
            group_name = "silver"
            asset_key = AssetKey(["silver", model_name])
        
        # Create eager auto-materialization policy
        eager_condition = AutomationCondition.eager()
        auto_materialize_policy = AutoMaterializePolicy.from_automation_condition(eager_condition)
        
        return AssetSpec(
            key=asset_key,
            deps=frozenset(dependencies),
            group_name=group_name,
            description=description,
            metadata=metadata,
            skippable=asset_spec.skippable,
            code_version=asset_spec.code_version,
            auto_materialize_policy=auto_materialize_policy,
        )
    
    def execute(
        self, context: AssetExecutionContext, dbt: DbtCliResource
    ) -> Iterator:
        """Execute dbt models.
        
        When a specific asset is materialized, run only that model (like dlt does per resource).
        This makes each dbt model appear as a separate step in Dagster UI.
        """
        # Get the model name from the asset key (e.g., ["silver", "dim_user"] -> "dim_user")
        asset_key = context.asset_key
        if len(asset_key.path) >= 2:
            # Asset key is like ["staging", "stg_lrs_events"] or ["silver", "dim_user"]
            # The model name is the last part of the path
            model_name = asset_key.path[-1]
            # Run only this specific model (dbt will handle dependencies automatically)
            # This makes each model run as a separate step, matching dlt's behavior
            yield from dbt.cli(["run", "--select", model_name], context=context).stream()
        else:
            # Fallback: if we can't determine the model name, run all selected models
            # This shouldn't happen, but provides a safety net
            yield from dbt.cli(["run", "--threads", "8"], context=context).stream()

