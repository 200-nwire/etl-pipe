"""Dagster asset definitions for the lineage project.

This module is automatically discovered by load_from_defs_folder.
All assets are imported here to ensure they're registered.
"""

# Import assets to register them with load_from_defs_folder
from lineage.defs.sources import (  # noqa: F401
    mongodb_source_asset,
    xapi_source_asset,
)
# NOTE: raw_tables assets are now created by dlt component (dlt_loads/defs.yaml)
# Removing raw_tables imports to avoid duplicates
# from lineage.defs.raw_tables import lms_raw_assets, lrs_raw_table_asset
# Removed: sync_duckdb_for_dbt and dbt_validation_tests (not needed)

# NOTE: dbt_assets are NOT imported here to avoid loading the large manifest.json
# at import time, which causes timeouts. They will be loaded lazily in definitions.py
