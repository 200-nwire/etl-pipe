"""Custom DagsterDbtTranslator to connect dbt sources to raw table assets."""

from dagster import AssetKey
from dagster_dbt import DagsterDbtTranslator

# Mapping from dbt source table names to actual raw MongoDB collection names
# This maps the silver table names (as they appear in dbt sources) to the raw collections
DBT_SOURCE_TO_RAW_MAPPING = {
    # Dimensions
    "dim_time": "events",  # Time dimension likely comes from events or is generated
    "dim_organization": "schools",  # Organizations come from schools collection
    "dim_learner": "users",  # Learners come from users collection
    "dim_teacher": "users",  # Teachers also from users (with role filter)
    "dim_course": "courses",  # Courses from courses collection
    "dim_section": "enrollments",  # Sections from enrollments or courses
    "dim_content_item": "pages",  # Content items from pages/blocks
    "dim_assessment": "assessment_profiles",  # Assessments from assessment_profiles
    "dim_skill": "skills",  # Skills from skills collection
    "dim_session": "login",  # Sessions from login/session data
    "dim_platform": "events",  # Platform from events metadata
    "dim_lti_tool": "events",  # LTI tools from events
    "dim_ai_tool": "events",  # AI tools from events
    
    # Facts
    "fact_learning_event": "events",  # Learning events from events collection
    "fact_assessment_attempt": "exercise_submissions",  # Assessment attempts from submissions
    "fact_learning_signal": "events",  # Learning signals from events
    "fact_variant_exposure": "events",  # Variant exposures from events
    "fact_ai_interaction": "events",  # AI interactions from events
    "fact_session_summary": "login",  # Session summaries from login/session
    "fact_lti_launch": "events",  # LTI launches from events
    
    # Reference tables
    "ref_event_type": "events",  # Event types from events
    "ref_verb": "events",  # Verbs from events (xAPI)
    "ref_signal_type": "events",  # Signal types from events
}

# xAPI sources
XAPI_SOURCE_MAPPING = {
    "xapi_statements": "xapi_statements",
}


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    """Custom translator that maps dbt sources to Dagster raw table asset keys."""
    
    def get_compute_kind(self, dbt_resource_props: dict) -> str:
        """Return 'dbt' as compute_kind for all dbt models to show dbt icon in UI."""
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "model":
            return "dbt"
        # Fallback to default if parent doesn't have this method
        try:
            return super().get_compute_kind(dbt_resource_props)
        except AttributeError:
            return "dbt"  # Default to dbt for all resources
    
    def get_asset_spec(self, manifest: dict, unique_id: str, project=None):
        """Get asset spec for a dbt resource, including dependencies on raw table assets.
        
        This connects silver models to raw table assets in the lineage graph.
        """
        # Get the base asset spec from parent
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
        
        # Extract dependencies from dbt model
        # Start fresh - don't use parent's deps as they may be incorrectly mapped
        dependencies = []
        depends_on = resource_props.get("depends_on", {})
        source_nodes = depends_on.get("nodes", [])
        
        model_name = resource_props.get("name", "")
        is_staging = model_name.startswith("stg_") or "staging" in resource_props.get("schema", "").lower()
        
        for node_id in source_nodes:
            # Check if this is a reference to another dbt model (staging -> silver dependency)
            if "model" in node_id:
                # Extract model name from node_id (format: model.schema.model_name)
                parts = node_id.split(".")
                if len(parts) >= 3:
                    ref_model_name = parts[-1]  # Last part is the model name
                    # If this is a silver model depending on a staging model, add staging asset key
                    if not is_staging and ref_model_name.startswith("stg_"):
                        staging_key = AssetKey(["staging", ref_model_name])
                        if staging_key not in dependencies:
                            dependencies.append(staging_key)
                continue
            
            # Handle source dependencies (raw -> staging or raw -> silver)
            if "source" in node_id:
                # Extract source name and table from node_id (format: source.raw_lms.lms_users)
                # Full format: source.{schema}.{source_name}.{table_name}
                parts = node_id.split(".")
                if len(parts) >= 4:  # source.schema.raw_lms.lms_users
                    source_name = parts[2]  # raw_lms
                    table_name = parts[3]   # lms_users
                elif len(parts) >= 3:  # source.raw_lms.lms_users (fallback)
                    source_name = parts[1]
                    table_name = parts[2]
                else:
                    continue
                    
                if source_name == "raw_lms":
                    # Map dbt source table names to raw asset keys
                    # Raw assets use key format: ["raw", "lms_users"] (not ["raw", "mongo", "users"])
                    if table_name.startswith("lms_"):
                        # Direct mapping: lms_users -> ["raw", "lms_users"]
                        raw_key = AssetKey(["raw", table_name])
                        if raw_key not in dependencies:
                            dependencies.append(raw_key)
                    else:
                        # Legacy mapping: dim_learner -> raw.lms_users (via mapping)
                        raw_collection = DBT_SOURCE_TO_RAW_MAPPING.get(table_name, table_name)
                        raw_key = AssetKey(["raw", f"lms_{raw_collection}"])
                        if raw_key not in dependencies:
                            dependencies.append(raw_key)
                elif source_name == "raw_lrs":
                    # LRS asset key format: ["raw", "lrs", "lrs_statements"]
                    raw_key = AssetKey(["raw", "lrs", "lrs_statements"])
                    if raw_key not in dependencies:
                        dependencies.append(raw_key)
        
        # Create new asset spec with dependencies and enhanced metadata
        from dagster import AssetSpec, MetadataValue, TableSchema, AutoMaterializePolicy, AutoMaterializeRule
        from lineage.schemas.silver_schemas import get_silver_table_metadata
        
        # Get description from resource_props (from schema.yml) - this is more reliable
        description = resource_props.get("description", asset_spec.description or "")
        
        # Build enhanced metadata with schema, lineage, and why/what info
        metadata = dict(asset_spec.metadata) if asset_spec.metadata else {}
        
        # Add comprehensive silver table metadata (schema, fields, why/what)
        table_name = resource_props.get("name", "")
        silver_metadata = get_silver_table_metadata(table_name, resource_props)
        metadata.update(silver_metadata)
        
        # Extract raw source dependencies and add to metadata
        raw_sources_list = []
        for node_id in source_nodes:
            if "source" in node_id:
                parts = node_id.split(".")
                if len(parts) >= 4:  # source.schema.raw_lms.lms_users
                    source_name = parts[2]  # raw_lms
                    table_name_raw = parts[3]   # lms_users
                elif len(parts) >= 3:  # source.raw_lms.lms_users (fallback)
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
        
        # Add raw sources to metadata if not already added
        if raw_sources_list and "raw_sources" not in metadata:
            metadata["raw_sources"] = MetadataValue.text(", ".join(raw_sources_list))
            metadata["lineage"] = MetadataValue.md(
                f"**Raw Sources**: {', '.join([f'`{s}`' for s in raw_sources_list])}"
            )
        
        # Add compute_kind for dbt assets - this shows the dbt icon in Dagster UI
        # compute_kind should be a string, not MetadataValue for proper icon display
        # The translator's parent class may already set this, but we ensure it's "dbt"
        # Note: compute_kind is set on the asset spec, not in metadata
        
        # Always return updated asset spec with dependencies (even if empty, to ensure descriptions are set)
        # Note: deps must be a frozenset for AssetSpec
        # Auto-materialization: dbt assets should materialize when raw assets update
        # Using AutomationCondition.eager() (recommended) instead of deprecated AutoMaterializePolicy.eager()
        from dagster import AutoMaterializePolicy, AutomationCondition
        
        # Create eager auto-materialization policy
        # This ensures dbt models materialize automatically when their upstream raw assets are updated
        eager_condition = AutomationCondition.eager()
        auto_materialize_policy = AutoMaterializePolicy.from_automation_condition(eager_condition)
        
        # Note: compute_kind is set via get_compute_kind() method which is called by @dbt_assets decorator
        # The @dbt_assets decorator uses the translator's get_compute_kind() to set compute_kind on the asset
        # We don't need to set it in AssetSpec - it's handled by the decorator
        
        return AssetSpec(
            key=asset_spec.key,
            deps=frozenset(dependencies),  # This connects silver to raw in lineage - must be frozenset
            group_name=self.get_group_name(resource_props),  # Use our custom group name logic
            description=description,  # Use description from schema.yml
            metadata=metadata,
            skippable=asset_spec.skippable,
            code_version=asset_spec.code_version,
            auto_materialize_policy=auto_materialize_policy,  # Auto-materialize when parents update
        )
    
    def get_asset_key(self, dbt_resource_props: dict) -> AssetKey:
        """Get asset key for dbt resource (model or source).
        
        For sources: Don't create asset keys - sources are just references to existing assets.
        For models: Uses staging or silver prefix based on model name/schema.
        """
        resource_type = dbt_resource_props.get("resource_type")
        name = dbt_resource_props.get("name")
        schema = dbt_resource_props.get("schema", "")
        
        # Only create asset keys for models, not sources
        # Sources are just references and shouldn't create duplicate assets
        if resource_type == "model":
            # Check if it's a staging model (starts with stg_ or schema contains staging)
            if name.startswith("stg_") or "staging" in schema.lower():
                return AssetKey(["staging", name])
            else:
                return AssetKey(["silver", name])
        
        # For sources, use default behavior (which may not create assets)
        # This prevents duplicate asset keys when multiple sources map to same raw table
        return super().get_asset_key(dbt_resource_props)
    
    def get_group_name(self, dbt_resource_props: dict) -> str:
        """Group dbt models under 'staging' or 'silver' group based on model name/schema."""
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "model":
            name = dbt_resource_props.get("name", "")
            schema = dbt_resource_props.get("schema", "")
            # Check if it's a staging model
            if name.startswith("stg_") or "staging" in schema.lower():
                return "staging"
            else:
                return "silver"
        return super().get_group_name(dbt_resource_props)
    
    def get_description(self, dbt_resource_props: dict) -> str:
        """Get description for dbt model with lineage information."""
        resource_type = dbt_resource_props.get("resource_type")
        name = dbt_resource_props.get("name")
        description = dbt_resource_props.get("description", "")
        
        if resource_type == "model":
            # Add lineage information showing which raw tables are used
            raw_sources = []
            depends_on = dbt_resource_props.get("depends_on", {})
            nodes = depends_on.get("nodes", [])
            
            for node_id in nodes:
                if "source" in node_id:
                    # Extract source name and table from node_id (format: source.raw_lms.lms_users)
                    parts = node_id.split(".")
                    if len(parts) >= 3:
                        source_name = parts[1] if len(parts) == 3 else parts[2]
                        table_name = parts[2] if len(parts) == 3 else parts[3]
                        if source_name == "raw_lms":
                            # Table names now have lms_ prefix (e.g., lms_users)
                            # Remove prefix to get collection name for mapping
                            collection_name = table_name.replace("lms_", "") if table_name.startswith("lms_") else table_name
                            raw_collection = DBT_SOURCE_TO_RAW_MAPPING.get(collection_name, collection_name)
                            raw_sources.append(f"`raw.lms_{raw_collection}`")
                        elif source_name == "raw_lrs":
                            raw_sources.append(f"`raw.lrs.{table_name}`")
            
            lineage_note = ""
            if raw_sources:
                lineage_note = f"\n\n**Raw Sources**: {', '.join(raw_sources)}"
            
            return f"{description}{lineage_note}" if description else f"Silver model: {name}{lineage_note}"
        
        return description or super().get_description(dbt_resource_props)
    
    def get_metadata(self, dbt_resource_props: dict) -> dict:
        """Get metadata for dbt resource, ensuring compute_kind is included.
        
        This method is called by @dbt_assets to set metadata on each asset.
        We ensure compute_kind info is included in metadata for visibility.
        Also adds comprehensive schema metadata for staging and silver models.
        
        IMPORTANT: compute_kind must be a plain string in metadata, not MetadataValue,
        for the dbt icon to appear in the UI.
        """
        # Get base metadata from parent
        metadata = super().get_metadata(dbt_resource_props) if hasattr(super(), 'get_metadata') else {}
        
        # Ensure compute_kind is in metadata for dbt models
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "model":
            # CRITICAL: compute_kind must be a plain string, not MetadataValue
            # This is what makes the dbt icon appear in the UI
            # The @dbt_assets decorator uses get_compute_kind() method, but if parent
            # doesn't have it, we set it here in metadata as fallback
            metadata["compute_kind"] = "dbt"
            metadata["tool"] = "dbt"
            
            # Add staging schema metadata if this is a staging model
            model_name = dbt_resource_props.get("name", "")
            if model_name.startswith("stg_") or "staging" in dbt_resource_props.get("schema", "").lower():
                from lineage.schemas.staging_schemas import get_staging_table_metadata
                staging_metadata = get_staging_table_metadata(model_name)
                # staging_metadata contains MetadataValue objects - merge them properly
                for key, value in staging_metadata.items():
                    metadata[key] = value
        
        return metadata

