"""Dagster definitions for the lineage project.

Uses load_from_defs_folder to automatically discover assets from the defs/ folder.
Resources are defined here and merged with discovered assets.
"""

import os
import shutil
from collections import OrderedDict
from pathlib import Path

from dagster import AssetsDefinition, Definitions, load_from_defs_folder
from dagster_dbt import DbtCliResource
from dotenv import load_dotenv

# Import schedules
from lineage.defs.schedules import raw_ingestion_schedule

# Import configurable resources
from lineage.resources import BigQueryConfig, LRSConfig, MongoDBConfig

# Load environment variables
env_path = Path(__file__).parent.parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

# Ensure BQ_DATASET_PREFIX is set - derive from GCP_BQ_DATASET if not explicitly set
# This ensures dbt subprocesses have access to the prefix
if "BQ_DATASET_PREFIX" not in os.environ:
    gcp_dataset = os.environ.get("GCP_BQ_DATASET", "")
    if gcp_dataset:
        # If GCP_BQ_DATASET is set (e.g., "test_lin1"), use it as prefix with underscore
        # This makes schemas like "test_lin1_silver", "test_lin1_staging"
        prefix = f"{gcp_dataset}_" if not gcp_dataset.endswith("_") else gcp_dataset
        os.environ["BQ_DATASET_PREFIX"] = prefix

# Get dbt target from environment or default to dev (BigQuery) for local dev
dbt_target = os.environ.get("DBT_TARGET", "dev")

# dbt project is in the parent directory (amit-etl/dbt)
# lineage/src/lineage/definitions.py -> lineage -> src -> lineage -> amit-etl -> dbt
# Path: __file__ is in lineage/src/lineage/, need to go up 3 levels to get to amit-etl
DBT_PROJECT_PATH = str(Path(__file__).parent.parent.parent.parent / "dbt")

# Find dbt executable
dbt_executable = shutil.which("dbt")
if not dbt_executable:
    # Try in venv
    venv_dbt = Path(__file__).parent.parent.parent / ".venv" / "bin" / "dbt"
    if venv_dbt.exists():
        dbt_executable = str(venv_dbt)
    else:
        # Fallback - let it try to find it
        dbt_executable = "dbt"

# Create dbt resource
dbt_resource = DbtCliResource(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROJECT_PATH,
    target=dbt_target,
    dbt_executable=dbt_executable,  # Specify explicit path
)


# Load assets automatically from defs/ folder
# load_from_defs_folder returns a Definitions object, but we extract assets/resources
# immediately to avoid having multiple Definitions objects at module scope
# Handle missing dbt manifest gracefully (e.g., in CI where dbt parse hasn't run)
try:
    _discovered_defs = load_from_defs_folder(path_within_project=Path(__file__).parent / "defs")
except Exception as e:
    # If dbt manifest is missing, skip dbt component and continue
    # This happens in CI when dbt parse hasn't been run
    error_str = str(e).lower()
    is_manifest_error = (
        "manifest.json" in str(e) or
        "manifest" in error_str or
        "dagsterdbtmanifestnotfound" in error_str
    )
    if is_manifest_error:
        import warnings
        warnings.warn(
            f"dbt manifest not found, skipping dbt assets: {e}. "
            "Run 'dbt parse' to generate manifest.json if dbt assets are needed."
        )
        # Load from defs folder but exclude dbt_models directory
        defs_path = Path(__file__).parent / "defs"
        # Temporarily rename dbt_models to skip it
        dbt_models_path = defs_path / "dbt_models"
        dbt_models_backup = None
        if dbt_models_path.exists():
            dbt_models_backup = defs_path / "dbt_models.bak"
            dbt_models_path.rename(dbt_models_backup)
        try:
            _discovered_defs = load_from_defs_folder(path_within_project=defs_path)
        finally:
            # Restore dbt_models directory
            if dbt_models_backup and dbt_models_backup.exists():
                dbt_models_backup.rename(dbt_models_path)
    else:
        # Re-raise if it's a different error
        raise

# NOTE: dbt assets are now loaded via DbtProjectComponent in defs/dbt_models/defs.yaml
# This creates individual assets per dbt model (not a single multi-asset)
# Each model appears separately in the UI with its own dbt icon
discovered_assets = list(_discovered_defs.assets)
discovered_resources = _discovered_defs.resources
# Clear the reference to avoid Dagster seeing multiple Definitions objects
del _discovered_defs

# Filter out duplicate source assets from dlt component
# The dlt component creates both source assets (mongo_source_lms_*) and raw assets (raw/lms_*)
# We only need the raw assets - the source assets are duplicates

def _should_filter_asset(asset_def: AssetsDefinition) -> bool:
    """Check if an asset should be filtered out.
    
    Filters out:
    1. Duplicate source assets (mongo_source_lms_*, rest_api_lrs_statements)
       - These duplicate the raw assets (raw/lms_*, raw/lrs/lrs_statements)
    2. Test assets (test_lin1/*)
       - These are test assets that aren't needed in production
    """
    # Get asset key(s) - single asset has 'key', multi-asset has 'keys'
    try:
        if hasattr(asset_def, 'keys') and asset_def.keys:
            if isinstance(asset_def.keys, (set, frozenset)):
                keys = list(asset_def.keys)
            else:
                keys = list(asset_def.keys)
        else:
            # Single asset - use 'key' attribute
            keys = [asset_def.key]
    except (AttributeError, TypeError):
        # Fallback - try to get key directly
        keys = [asset_def.key] if hasattr(asset_def, 'key') else []
    
    for key in keys:
        if len(key.path) > 0:
            first_part = key.path[0]
            
            # Filter out duplicate source assets (duplicate of raw assets)
            if first_part.startswith("mongo_source_") or first_part == "rest_api_lrs_statements":
                return True
            
            # Filter out test assets
            if first_part.startswith("test_") or first_part == "test_lin1":
                return True
    
    return False

# Filter out unwanted assets (duplicate source assets and test assets)
# Also deduplicate by asset key to prevent duplicates
seen_keys = OrderedDict()
filtered_assets = []
for asset in discovered_assets:
    if _should_filter_asset(asset):
        continue
    
    # Get all keys for this asset
    if hasattr(asset, 'keys') and asset.keys:
        keys = list(asset.keys) if isinstance(asset.keys, (set, frozenset)) else list(asset.keys)
    else:
        keys = [asset.key] if hasattr(asset, 'key') else []
    
    # Check if any key is a duplicate
    is_duplicate = False
    for key in keys:
        key_str = str(key)
        if key_str in seen_keys:
            print(f"⚠️  Filtering duplicate asset: {key_str}")
            is_duplicate = True
            break
        seen_keys[key_str] = asset
    
    if not is_duplicate:
        filtered_assets.append(asset)

discovered_assets = filtered_assets

# Post-process assets to set group_name based on asset key
# This ensures assets are grouped correctly (raw, silver, staging, sources)
# and eliminates the "default" group

def _set_asset_group(asset_def: AssetsDefinition) -> AssetsDefinition:
    """Set group_name for an asset based on its key.
    
    Assets are grouped by the first part of their key:
    - ['raw', ...] -> 'raw'
    - ['silver', ...] -> 'silver'
    - ['staging', ...] -> 'staging'
    - ['source', ...] or keys starting with 'mongo_source_' or 'source_' -> 'sources'
    - ['test_*'] -> keep in default (test assets)
    - Otherwise -> keep existing group or None
    """
    # Get asset keys - convert to list if it's a set
    if hasattr(asset_def, 'keys'):
        if isinstance(asset_def.keys, (set, frozenset)):
            keys = list(asset_def.keys)
        else:
            keys = list(asset_def.keys)
    else:
        keys = [asset_def.key]
    
    # Build mapping of keys to group names
    group_names_by_key = {}
    for key in keys:
        key_str = str(key)
        if len(key.path) > 0:
            first_part = key.path[0]
            
            # Map key prefix to group name
            if first_part == "raw":
                group_names_by_key[key] = "raw"
            elif first_part == "silver":
                group_names_by_key[key] = "silver"
            elif first_part == "staging":
                group_names_by_key[key] = "staging"
            elif first_part == "source":
                group_names_by_key[key] = "sources"
            elif first_part.startswith("mongo_source_") or first_part.startswith("rest_api_"):
                # dlt component creates source assets with keys like 'mongo_source_lms_rules'
                # or 'rest_api_lrs_statements'
                group_names_by_key[key] = "sources"
            elif ("source" in key_str.lower() and
                  first_part not in ["raw", "silver", "staging", "test_lin1"]):
                # Catch any other source patterns (but not test assets)
                group_names_by_key[key] = "sources"
            # Skip test_* assets - leave them in default
            # If no mapping, keep existing group (don't add to dict)
    
    # Apply group names using with_attributes
    # NOTE: Dagster's with_attributes cannot override existing group_name
    # Assets from dlt component have group_name=None which defaults to "default"
    # We try to set groups, but it may not work for all assets
    # The proper fix would be to configure DltLoadCollectionComponent to set
    # group_name at creation time, but that's not currently supported
    if group_names_by_key and hasattr(asset_def, 'with_attributes'):
        try:
            # Try to set group names - this works for assets that don't have
            # an explicit group_name set (group_name=None can sometimes be overridden)
            return asset_def.with_attributes(group_names_by_key=group_names_by_key)
        except Exception:
            # If modification fails, return original
            # This is expected for assets with existing group_name from components
            pass
    
    return asset_def

# Apply group_name to all discovered assets
discovered_assets = [_set_asset_group(asset) for asset in discovered_assets]

# Note: Auto-materialization for dbt assets is configured via the Dagster instance
# or can be enabled per-asset in the UI. The policy is set at the instance level
# or via the AssetDaemon configuration.

# Create configurable resources that can be set from UI
# These use environment variables as defaults, but can be overridden per-run
bigquery_config = BigQueryConfig.configure_at_launch()
lrs_config = LRSConfig.configure_at_launch()
mongo_config = MongoDBConfig.configure_at_launch()

# Merge with resources and schedules
# NOTE: dbt assets are automatically discovered from defs/dbt_models/defs.yaml
# via load_from_defs_folder, so they're already in discovered_assets
defs = Definitions(
    assets=discovered_assets,  # Includes dbt assets from component
    resources={
        **discovered_resources,
        "dbt": dbt_resource,  # dbt resource for executing dbt commands
        "bq_config": bigquery_config,  # Configurable from UI
        "lrs_config": lrs_config,  # Configurable from UI
        "mongo_config": mongo_config,  # Configurable from UI
    },
    schedules=[raw_ingestion_schedule],
)
