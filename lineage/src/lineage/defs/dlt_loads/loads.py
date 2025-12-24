"""dlt sources and pipelines for LMS and LRS ingestion.

This module defines dlt sources and pipelines that are used by DltLoadCollectionComponent
to create individual Dagster assets per dlt resource.
"""

import os
from typing import List, Optional

import dlt
from dlt.destinations.impl.bigquery.factory import bigquery

# LRS functions are defined inline below
from lineage.resources import BigQueryConfig, LRSConfig, MongoDBConfig
from lineage.sources.lms import (
    COLLECTION_TO_TABLE_MAP,
    DEFAULT_MONGO_COLLECTIONS,
    _convert_mongo_doc,
    _get_mongo_client,
)


def create_lms_source(
    collections: Optional[List[str]] = None,
    mongo_config: Optional[MongoDBConfig] = None,
):
    """Create dlt source for MongoDB LMS collections.
    
    Args:
        collections: List of collection names to load. If None, loads all default collections.
        mongo_config: MongoDB configuration resource.
    
    Returns:
        dlt source with resources for each collection.
    
    Note: This creates a source that yields multiple resources (one per collection).
    DltLoadCollectionComponent will create individual Dagster assets for each resource.
    """
    selected_collections = collections or DEFAULT_MONGO_COLLECTIONS
    
    # Get MongoDB client and database
    if mongo_config:
        from pymongo import MongoClient
        connection_string = mongo_config.get_connection_string()
        db_name = mongo_config.get_database_name()
        client = MongoClient(
            connection_string,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=20000,
            socketTimeoutMS=20000,
            readPreference='primaryPreferred',
            retryWrites=True,
            retryReads=True,
        )
    else:
        client = _get_mongo_client()
        mongo_uri = os.environ.get("MONGO_URI", "")
        db_name = os.environ.get("MONGO_DATABASE")
        
        if mongo_uri and not db_name:
            try:
                uri_parts = mongo_uri.split("/")
                if len(uri_parts) > 3:
                    potential_db = uri_parts[-1].split("?")[0].split("&")[0]
                    if potential_db and potential_db.strip():
                        db_name = potential_db.strip()
            except Exception:
                pass
        
        if not db_name or not db_name.strip():
            # Don't use "default" - raise error instead to force proper configuration
            raise RuntimeError(
                "MONGO_DATABASE environment variable is required. "
                "Either set MONGO_DATABASE or include database name in MONGO_URI."
            )
    
    db = client[db_name]
    
    @dlt.source
    def mongo_source():
        """dlt source for MongoDB LMS collections."""
        for collection_name in selected_collections:
            # Map collection name to table name
            mapped_name = COLLECTION_TO_TABLE_MAP.get(collection_name, collection_name)
            mapped_name = mapped_name.replace("-", "_")
            table_name = f"lms_{mapped_name}"
            
            # Use dlt's incremental loading
            incremental = dlt.sources.incremental(
                "modified_on",
                initial_value="1970-01-01T00:00:00Z",
            )
            
            @dlt.resource(
                name=table_name,
                write_disposition="merge",
                primary_key="_id",
                max_table_nesting=0,  # Preserve nested structures as BigQuery RECORD types
                columns={},
            )
            def collection_resource(incremental=incremental):
                """Resource for a single MongoDB collection."""
                collection = db[collection_name]
                
                # Build query for incremental loading
                query = {}
                if incremental.last_value:
                    from dateutil.parser import isoparse
                    try:
                        if isinstance(incremental.last_value, str):
                            last_value_dt = isoparse(incremental.last_value)
                        else:
                            last_value_dt = incremental.last_value
                    except (ValueError, TypeError):
                        from datetime import datetime
                        last_value_dt = datetime(1970, 1, 1)
                    
                    sample = collection.find_one(
                        {},
                        {
                            "modified_on": 1,
                            "updatedAt": 1,
                            "updated_at": 1,
                            "modifiedAt": 1,
                        },
                    )
                    if sample:
                        if "modified_on" in sample:
                            query = {"modified_on": {"$gt": last_value_dt}}
                        elif "updatedAt" in sample:
                            query = {"updatedAt": {"$gt": last_value_dt}}
                        elif "updated_at" in sample:
                            query = {"updated_at": {"$gt": last_value_dt}}
                        elif "modifiedAt" in sample:
                            query = {"modifiedAt": {"$gt": last_value_dt}}
                
                cursor = collection.find(
                    query,
                    no_cursor_timeout=True,
                ).batch_size(5000)
                
                for doc in cursor:
                    yield _convert_mongo_doc(doc)
            
            yield collection_resource
    
    return mongo_source()


def create_lrs_source(
    lrs_config: Optional[LRSConfig] = None,
):
    """Create dlt source for xAPI LRS statements.
    
    Args:
        lrs_config: LRS configuration resource.
    
    Returns:
        dlt source for LRS statements.
    """
    from urllib.parse import urlparse, urlunparse

    from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
    
    if lrs_config:
        endpoint = lrs_config.endpoint
        auth_token = lrs_config.auth_token.get_secret_value() if lrs_config.auth_token else None
    else:
        endpoint = os.environ.get("XAPI_LRS_ENDPOINT")
        if not endpoint:
            raise RuntimeError("XAPI_LRS_ENDPOINT is required for xAPI ingestion")
        auth_token = os.environ.get("XAPI_LRS_AUTH_TOKEN")
    
    if not endpoint:
        raise RuntimeError("XAPI_LRS_ENDPOINT is required for xAPI ingestion")
    
    # Normalize endpoint
    base_endpoint = endpoint.rstrip("/")
    if not base_endpoint.endswith("/statements"):
        base_endpoint = base_endpoint + "/statements"
    
    parsed = urlparse(base_endpoint)
    base_url = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
    
    # Configure headers
    client_headers = {
        "X-Experience-API-Version": "1.0.3",
        "Content-Type": "application/json",
    }
    
    if auth_token:
        if not auth_token.startswith("Basic "):
            client_headers["Authorization"] = f"Basic {auth_token}"
        else:
            client_headers["Authorization"] = auth_token
    
    config: RESTAPIConfig = {
        "client": {
            "base_url": base_url,
            "headers": client_headers,
            "paginator": {
                "type": "json_link",
                "next_url_path": "more",
            },
        },
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "merge",
            "max_table_nesting": 0,  # Preserve nested structures
        },
        "resources": [
            {
                "name": "lrs_statements",
                "endpoint": {
                    "path": parsed.path,
                    "data_selector": "statements",
                    "params": {
                        "limit": 500,
                    },
                    "incremental": {
                        "cursor_path": "stored",
                        "initial_value": "1970-01-01T00:00:00Z",
                    },
                },
            },
        ],
    }
    
    return rest_api_source(config)


def create_lms_pipeline(
    bq_config: Optional[BigQueryConfig] = None,
    dataset_name: Optional[str] = None,
) -> dlt.Pipeline:
    """Create dlt pipeline for LMS data.
    
    Args:
        bq_config: BigQuery configuration resource.
        dataset_name: Dataset name. If None, uses bq_config or env vars.
    
    Returns:
        dlt pipeline configured for BigQuery.
    """
    if dataset_name is None:
        if bq_config:
            dataset_name = bq_config.get_raw_dataset()
        else:
            # Use BQ_DATASET_PREFIX if set, otherwise default to "raw"
            prefix = os.environ.get("BQ_DATASET_PREFIX", "").strip()
            if prefix:
                # If prefix is set, use it with "raw" (e.g., "test_lin1_" -> "test_lin1_raw")
                if not prefix.endswith("_"):
                    prefix = prefix + "_"
                dataset_name = f"{prefix}raw"
            else:
                # If no prefix, use "raw" (not GCP_BQ_DATASET which is for dbt base dataset)
                dataset_name = "raw"
    
    # Get location from bq_config or environment variable
    location = None
    if bq_config:
        location = bq_config.location
    else:
        location = os.environ.get("BQ_LOCATION", "me-west1")
    
    # Configure BigQuery destination with credentials
    # dlt reads credentials from .dlt/secrets.toml or environment variables
    # We create/update secrets.toml from the JSON file to ensure credentials are available
    # when dlt recreates destination clients during pipeline.load()
    import json
    from pathlib import Path
    
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/gcp_credentials.json")
    if os.path.exists(creds_path):
        # Load credentials from JSON file
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
        
        # Create/update .dlt/secrets.toml from JSON file
        # This ensures dlt can read credentials when recreating destination clients
        dlt_secrets_dir = Path(__file__).parent.parent.parent.parent / ".dlt"
        dlt_secrets_dir.mkdir(exist_ok=True)
        secrets_file = dlt_secrets_dir / "secrets.toml"
        
        # Write secrets.toml with credentials from JSON
        # Escape private key for TOML (escape quotes)
        private_key_escaped = creds_data.get("private_key", "").replace('"', '\\"')
        project_id = creds_data.get("project_id", "")
        client_email = creds_data.get("client_email", "")
        pipeline_name_lower = "lms_raw"  # Pipeline name for LMS
        
        # dlt looks for credentials in multiple places:
        # 1. Pipeline-specific: lms_raw.destination.bigquery.credentials.*
        # 2. Generic: destination.bigquery.credentials.*
        # We include both to ensure credentials are found
        secrets_content = f'''# dlt secrets configuration
# Auto-generated from GOOGLE_APPLICATION_CREDENTIALS JSON file
# This file is updated dynamically to ensure credentials are available
# when dlt recreates destination clients during pipeline.load()

# Generic destination configuration (used as fallback)
[destination.bigquery]
location = "{location}"

[destination.bigquery.credentials]
project_id = "{project_id}"
client_email = "{client_email}"
private_key = """{private_key_escaped}"""

# Pipeline-specific configuration ({pipeline_name_lower} pipeline)
[{pipeline_name_lower}.destination.bigquery]
location = "{location}"

[{pipeline_name_lower}.destination.bigquery.credentials]
project_id = "{project_id}"
client_email = "{client_email}"
private_key = """{private_key_escaped}"""
'''
        secrets_file.write_text(secrets_content)
        
        # Create credentials object for direct use (for initial destination creation)
        from dlt.common.configuration.specs.gcp_credentials import GcpServiceAccountCredentials
        credentials = GcpServiceAccountCredentials(
            project_id=creds_data.get("project_id"),
            private_key=creds_data.get("private_key"),
            client_email=creds_data.get("client_email"),
        )
        destination = bigquery(credentials=credentials, location=location)
    else:
        # Fallback: let dlt try to use ADC or other methods
        destination = bigquery(location=location)
    
    return dlt.pipeline(
        pipeline_name="lms_raw",
        dataset_name=dataset_name,
        dev_mode=False,
        destination=destination,
    )


def create_lrs_pipeline(
    bq_config: Optional[BigQueryConfig] = None,
    dataset_name: Optional[str] = None,
) -> dlt.Pipeline:
    """Create dlt pipeline for LRS data.
    
    Args:
        bq_config: BigQuery configuration resource.
        dataset_name: Dataset name. If None, uses bq_config or env vars.
    
    Returns:
        dlt pipeline configured for BigQuery.
    """
    if dataset_name is None:
        if bq_config:
            dataset_name = bq_config.get_raw_dataset()
        else:
            # Use BQ_DATASET_PREFIX if set, otherwise default to "raw"
            prefix = os.environ.get("BQ_DATASET_PREFIX", "").strip()
            if prefix:
                # If prefix is set, use it with "raw" (e.g., "test_lin1_" -> "test_lin1_raw")
                if not prefix.endswith("_"):
                    prefix = prefix + "_"
                dataset_name = f"{prefix}raw"
            else:
                # If no prefix, use "raw" (not GCP_BQ_DATASET which is for dbt base dataset)
                dataset_name = "raw"
    
    # Get location from bq_config or environment variable
    location = None
    if bq_config:
        location = bq_config.location
    else:
        location = os.environ.get("BQ_LOCATION", "me-west1")
    
    # Configure BigQuery destination with credentials
    # dlt reads credentials from .dlt/secrets.toml or environment variables
    # We create/update secrets.toml from the JSON file to ensure credentials are available
    # when dlt recreates destination clients during pipeline.load()
    import json
    from pathlib import Path
    
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/gcp_credentials.json")
    if os.path.exists(creds_path):
        # Load credentials from JSON file
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
        
        # Create/update .dlt/secrets.toml from JSON file
        # This ensures dlt can read credentials when recreating destination clients
        dlt_secrets_dir = Path(__file__).parent.parent.parent.parent / ".dlt"
        dlt_secrets_dir.mkdir(exist_ok=True)
        secrets_file = dlt_secrets_dir / "secrets.toml"
        
        # Write secrets.toml with credentials from JSON
        # Escape private key for TOML (escape quotes)
        private_key_escaped = creds_data.get("private_key", "").replace('"', '\\"')
        project_id = creds_data.get("project_id", "")
        client_email = creds_data.get("client_email", "")
        pipeline_name_lower = "lrs_raw"  # Pipeline name for LRS
        
        # dlt looks for credentials in multiple places:
        # 1. Pipeline-specific: lrs_raw.destination.bigquery.credentials.*
        # 2. Generic: destination.bigquery.credentials.*
        # We include both to ensure credentials are found
        secrets_content = f'''# dlt secrets configuration
# Auto-generated from GOOGLE_APPLICATION_CREDENTIALS JSON file
# This file is updated dynamically to ensure credentials are available
# when dlt recreates destination clients during pipeline.load()

# Generic destination configuration (used as fallback)
[destination.bigquery]
location = "{location}"

[destination.bigquery.credentials]
project_id = "{project_id}"
client_email = "{client_email}"
private_key = """{private_key_escaped}"""

# Pipeline-specific configuration ({pipeline_name_lower} pipeline)
[{pipeline_name_lower}.destination.bigquery]
location = "{location}"

[{pipeline_name_lower}.destination.bigquery.credentials]
project_id = "{project_id}"
client_email = "{client_email}"
private_key = """{private_key_escaped}"""
'''
        secrets_file.write_text(secrets_content)
        
        # Create credentials object for direct use (for initial destination creation)
        from dlt.common.configuration.specs.gcp_credentials import GcpServiceAccountCredentials
        credentials = GcpServiceAccountCredentials(
            project_id=creds_data.get("project_id"),
            private_key=creds_data.get("private_key"),
            client_email=creds_data.get("client_email"),
        )
        destination = bigquery(credentials=credentials, location=location)
    else:
        # Fallback: let dlt try to use ADC or other methods
        destination = bigquery(location=location)
    
    return dlt.pipeline(
        pipeline_name="lrs_raw",
        dataset_name=dataset_name,
        dev_mode=False,
        destination=destination,
    )


# Create sources and pipelines for DltLoadCollectionComponent
# These will be referenced in defs.yaml
# NOTE: The component resolver does isinstance() type checking, so we need actual
# dlt source and pipeline objects, not wrappers. We create them eagerly but handle
# missing env vars by using placeholder values that won't cause errors at import time.
# The actual connection will happen when the pipeline runs.
# 
# IMPORTANT: Use module-level caching to ensure sources are singletons and not recreated
# on each import, which would cause duplicate assets in Dagster.

# Module-level cache for sources and pipelines
_lms_source_cache = None
_lrs_source_cache = None
_lms_pipeline_cache = None
_lrs_pipeline_cache = None

def _get_lms_source():
    """Get or create LMS source (singleton)."""
    global _lms_source_cache
    if _lms_source_cache is None:
        _lms_source_cache = create_lms_source()
    return _lms_source_cache

def _get_lrs_source():
    """Get or create LRS source (singleton)."""
    global _lrs_source_cache
    if _lrs_source_cache is None:
        _lrs_source_cache = create_lrs_source()
    return _lrs_source_cache

def _get_lms_pipeline():
    """Get or create LMS pipeline (singleton)."""
    global _lms_pipeline_cache
    if _lms_pipeline_cache is None:
        _lms_pipeline_cache = create_lms_pipeline()
    return _lms_pipeline_cache

def _get_lrs_pipeline():
    """Get or create LRS pipeline (singleton)."""
    global _lrs_pipeline_cache
    if _lrs_pipeline_cache is None:
        _lrs_pipeline_cache = create_lrs_pipeline()
    return _lrs_pipeline_cache

# Ensure env vars have defaults for import-time creation
# These placeholders allow the dlt source/pipeline objects to be created at import time
# The actual connection happens when the pipeline runs, not when objects are created
_original_mongo_uri = os.environ.get("MONGO_URI")
_original_mongo_database = os.environ.get("MONGO_DATABASE")
_original_lrs_endpoint = os.environ.get("XAPI_LRS_ENDPOINT")

# Set placeholders if not set (to allow object creation)
if not _original_mongo_uri:
    os.environ.setdefault("MONGO_URI", "mongodb://placeholder/placeholder_db")
if not _original_mongo_database:
    os.environ.setdefault("MONGO_DATABASE", "placeholder_db")
if not _original_lrs_endpoint:
    os.environ.setdefault("XAPI_LRS_ENDPOINT", "https://placeholder/statements")

try:
    # Create sources and pipelines eagerly using singleton pattern
    # These will work even with placeholder env vars - the actual connection
    # happens when the pipeline runs, not when the objects are created
    lms_source = _get_lms_source()
    lrs_source = _get_lrs_source()
    lms_pipeline = _get_lms_pipeline()
    lrs_pipeline = _get_lrs_pipeline()
finally:
    # Restore original env vars (if they weren't set, remove the placeholder)
    if _original_mongo_uri:
        os.environ["MONGO_URI"] = _original_mongo_uri
    elif "MONGO_URI" in os.environ and "placeholder" in os.environ.get("MONGO_URI", ""):
        del os.environ["MONGO_URI"]
    
    if _original_mongo_database:
        os.environ["MONGO_DATABASE"] = _original_mongo_database
    elif "MONGO_DATABASE" in os.environ and os.environ.get("MONGO_DATABASE") == "placeholder_db":
        del os.environ["MONGO_DATABASE"]
    
    if _original_lrs_endpoint:
        os.environ["XAPI_LRS_ENDPOINT"] = _original_lrs_endpoint
    elif ("XAPI_LRS_ENDPOINT" in os.environ and
          "placeholder" in os.environ.get("XAPI_LRS_ENDPOINT", "")):
        del os.environ["XAPI_LRS_ENDPOINT"]

