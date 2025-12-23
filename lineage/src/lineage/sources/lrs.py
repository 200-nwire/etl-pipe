"""dlt pipeline for ingesting LRS (xAPI) statements using dlt's REST API source."""

from __future__ import annotations

import os
from typing import Dict

import dlt
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source
from google.cloud import bigquery as bq_client


def load_xapi_raw(
    *,
    destination: str | None = None,
    dataset_name: str | None = None,
    pipeline_kwargs: Dict | None = None,
    since: str | None = None,
    bq_config: object | None = None,  # BigQueryConfig resource
    lrs_config: object | None = None,  # LRSConfig resource
) -> str:
    """Load xAPI statements into the raw dataset using dlt's REST API source.
    
    Uses dlt's built-in REST API source which handles:
    - Automatic pagination (via 'more' URL in response)
    - Authentication (Basic Auth)
    - Incremental loading (via 'since' parameter)
    - Error handling and retries
    
    Uses BigQuery as the destination. BigQuery natively supports nested and repeated fields (RECORD types).
    With max_table_nesting=0, dlt preserves nested structures as RECORD types instead of flattening.
    
    Configuration can be provided via:
    - LRSConfig resource (configurable from UI)
    - BigQueryConfig resource (configurable from UI)
    - Environment variables: XAPI_LRS_ENDPOINT, XAPI_AUTH_TOKEN, BQ_DATASET_PREFIX
    
    Args:
        destination: Ignored (always uses BigQuery)
        dataset_name: Dataset/schema name for the destination (defaults to bq_config.get_raw_dataset() or env var)
        pipeline_kwargs: Additional pipeline arguments
        since: ISO 8601 timestamp for incremental loading. If None, uses dlt state to
               track last successful run timestamp automatically.
        bq_config: BigQueryConfig resource (optional, uses env vars if not provided)
        lrs_config: LRSConfig resource (optional, uses env vars if not provided)
    
    Returns:
        The dataset name where data was loaded
    """
    # Get endpoint and auth from config resource or environment
    if lrs_config:
        endpoint = lrs_config.endpoint
        auth_token = lrs_config.auth_token
    else:
        endpoint = os.environ.get("XAPI_LRS_ENDPOINT")
        auth_token = os.environ.get("XAPI_AUTH_TOKEN")
    
    if not endpoint:
        raise RuntimeError("XAPI_LRS_ENDPOINT is required for xAPI ingestion (provide via LRSConfig or env var)")

    # Normalize endpoint - ensure it ends with /statements
    if lrs_config:
        base_endpoint = lrs_config.get_base_endpoint()
    else:
        base_endpoint = endpoint.rstrip("/")
        if not base_endpoint.endswith("/statements"):
            base_endpoint = base_endpoint + "/statements"
    
    # Extract base URL (scheme + netloc) for pagination
    from urllib.parse import urlparse, urlunparse
    parsed = urlparse(base_endpoint)
    base_url = urlunparse((parsed.scheme, parsed.netloc, "", "", "", ""))
    
    # Configure authentication
    # xAPI LRS uses Basic Auth - token can be:
    # 1. Base64 encoded "username:password" (most common)
    # 2. Plain "username:password" string
    # We'll use BearerTokenAuth if token is provided, or HttpBasicAuth if we can parse username:password
    auth_config = None
    if auth_token:
        from dlt.sources.helpers.rest_client.auth import HttpBasicAuth, BearerTokenAuth
        # Try to parse as username:password
        if ":" in auth_token and not auth_token.startswith("Basic "):
            parts = auth_token.split(":", 1)
            auth_config = HttpBasicAuth(username=parts[0], password=parts[1])
        else:
            # Assume it's already formatted for Authorization header (e.g., "Basic <base64>" or just base64)
            # Use BearerTokenAuth as a workaround, but we'll set it in headers instead
            # Actually, let's use a custom approach - set Authorization header directly
            # For now, use BearerTokenAuth and we'll override in headers
            auth_config = BearerTokenAuth(token=auth_token)
    
    # Get dataset name from config resource or environment
    if dataset_name is None:
        if bq_config:
            dataset_name = bq_config.get_raw_dataset()
        else:
            # Fallback to environment variables
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
    
    # Get last timestamp from dlt state for incremental loading
    pipeline_args = {
        "pipeline_name": "lrs_raw",
        "dataset_name": dataset_name,
        "full_refresh": False,
        **(pipeline_kwargs or {}),
    }
    
    # Create temporary pipeline to access state
    temp_pipeline = dlt.pipeline(**pipeline_args)
    state = temp_pipeline.state
    last_timestamp = since or state.get("sources", {}).get("lrs_statements", {}).get("last_timestamp")
    
    # Configure REST API source for xAPI LRS
    # xAPI LRS specification:
    # - Returns {"statements": [...], "more": "..."} format OR direct array [{"id": ...}, ...]
    # - Pagination via 'more' URL in JSON response
    # - Requires X-Experience-API-Version header
    # - Supports 'since' parameter for incremental loading
    # - Uses Basic Auth (Authorization: Basic <base64>)
    client_headers = {
        "X-Experience-API-Version": "1.0.3",
        "Content-Type": "application/json",
    }
    
    # Add Basic Auth header if token provided
    if auth_token:
        # If token doesn't start with "Basic ", add it
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
                "next_url_path": "more",  # xAPI LRS returns 'more' field with next page URL
            },
        },
        "resource_defaults": {
            "primary_key": "id",  # xAPI statements have 'id' field
            "write_disposition": "merge",
            "max_table_nesting": 0,  # Preserve nested structures as BigQuery RECORD types
        },
        "resources": [
            {
                "name": "lrs_statements",
                "endpoint": {
                    "path": parsed.path,  # e.g., "/trax/api/gateway/clients/default/stores/default/xapi/statements"
                    # data_selector: xAPI LRS can return either:
                    # 1. Direct array: [{...}, {...}] - use "$" (root)
                    # 2. Object: {"statements": [...], "more": "..."} - use "statements"
                    # We'll use "statements" as default (standard format), but dlt will auto-detect if needed
                    "data_selector": "statements",  # Extract statements array from response
                    "params": {
                        "limit": 500,  # xAPI LRS page size
                        # Use placeholder for incremental loading
                        "since": "{incremental.start_value}" if last_timestamp else None,
                    },
                    "incremental": {
                        "cursor_path": "stored",  # Track 'stored' timestamp field in each statement
                        "initial_value": last_timestamp or "1970-01-01T00:00:00Z",
                    },
                },
                # Preserve JSON structures instead of flattening
            },
        ],
    }
    
    # Remove None values from params
    if config["resources"][0]["endpoint"]["params"]["since"] is None:
        del config["resources"][0]["endpoint"]["params"]["since"]
        # Also remove incremental config if no initial value
        if not last_timestamp:
            del config["resources"][0]["endpoint"]["incremental"]
    
    # Create REST API source
    source = rest_api_source(config)
    
    # Always use BigQuery (remove DuckDB support)
    from dlt.destinations.impl.bigquery.factory import bigquery
    
    # Configure BigQuery destination with location
    # Get location from bq_config or environment variable
    location = None
    if bq_config:
        location = bq_config.location
    else:
        location = os.environ.get("BQ_LOCATION", "me-west1")
    
    # CRITICAL: Ensure dataset exists before dlt tries to load
    # dlt should create datasets automatically, but there may be permission or timing issues
    # Creating it explicitly ensures it exists before any load operations
    project = os.environ.get("GCP_PROJECT")
    if project:
        try:
            client = bq_client.Client(project=project, location=location)
            dataset_ref = client.dataset(dataset_name)
            try:
                # Check if dataset exists
                existing = client.get_dataset(dataset_ref)
                print(f"✓ Dataset {dataset_name} already exists in {location}")
            except Exception:
                # Create dataset if it doesn't exist
                print(f"⚠ Dataset {dataset_name} does not exist - creating...")
                dataset = bq_client.Dataset(dataset_ref)
                dataset.location = location
                dataset.description = f"Raw data from dlt pipelines (auto-created)"
                dataset = client.create_dataset(dataset, exists_ok=False)
                print(f"✓ Successfully created dataset: {dataset_name} in {location}")
        except Exception as e:
            print(f"⚠ Could not ensure dataset exists: {e}")
            print("  dlt will attempt to create it automatically")
    
    # BigQuery natively supports JSON type - dlt will preserve nested structures automatically
    # Configure BigQuery destination with credentials
    # dlt requires explicit credentials, not just GOOGLE_APPLICATION_CREDENTIALS env var
    from dlt.common.configuration.specs.gcp_credentials import GcpServiceAccountCredentials
    import json
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/gcp_credentials.json")
    if os.path.exists(creds_path):
        # Load credentials from JSON file
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
        # Extract only the fields needed by GcpServiceAccountCredentials
        credentials = GcpServiceAccountCredentials(
            project_id=creds_data.get("project_id"),
            private_key=creds_data.get("private_key"),
            client_email=creds_data.get("client_email"),
        )
        destination = bigquery(credentials=credentials, location=location)
    else:
        # Fallback: let dlt try to use ADC or other methods
        destination = bigquery(location=location)
    
    # Update pipeline args with destination
    pipeline_args["destination"] = destination
    # Note: data_types is not a valid parameter for dlt.pipeline()
    # dlt automatically preserves nested structures as JSON in BigQuery
    
    # Create pipeline
    # With max_table_nesting=0 in resource_defaults, dlt preserves nested structures
    # xAPI statements will be stored as BigQuery RECORD types (native nested fields)
    # This preserves the full xAPI statement structure without flattening
    pipeline = dlt.pipeline(**pipeline_args)
    
    # CRITICAL: ALWAYS drop pending packages BEFORE extracting
    # This prevents schema mismatch errors from previous runs
    # pipeline.extract() might automatically try to load pending packages, causing KeyErrors
    # We must drop them unconditionally to ensure a clean state
    try:
        # has_pending_data is a property, not a method
        # Always try to drop, even if has_pending_data is False (might be stale)
        # This ensures we start with a clean state
        print("⚠ Checking for pending packages...")
        if pipeline.has_pending_data:
            print("⚠ Found pending packages. Dropping them to avoid schema mismatch errors...")
            pipeline.drop_pending_packages()
            print("✓ Dropped pending packages")
        else:
            # Even if has_pending_data is False, try to drop anyway
            # This handles edge cases where the property might be stale
            try:
                pipeline.drop_pending_packages()
                print("✓ Cleared any stale pending packages")
            except Exception:
                # If drop fails when there are no packages, that's fine
                pass
    except Exception as e:
        # If dropping fails, log but continue - might be a new pipeline
        print(f"Note: Could not check/drop pending packages: {e}")
    
    # Extract and load separately to avoid pending package issues
    # pipeline.run() automatically tries to load pending packages, which causes errors
    # By using extract() and load() separately, we have more control
    
    # Extract data first (creates new pending packages)
    # Note: We already dropped old pending packages above, so extract will only create new ones
    print("⚠ Starting pipeline.extract()...")
    extract_info = pipeline.extract(source)
    print(f"✓ pipeline.extract() completed")
    
    # Check if extract created any pending packages
    if pipeline.has_pending_data:
        print(f"✓ Extract created pending packages - ready to load")
    else:
        print(f"⚠️  WARNING: Extract did NOT create any pending packages!")
        print(f"     This means no data was extracted, so load() will have nothing to load")
        print(f"     Check if xAPI LRS endpoint is returning data or if there are connection issues")
    
    # Then load (will load the newly extracted data)
    # Since we dropped old pending packages before extract, load() will only see the new packages
    print("⚠ Starting pipeline.load()...")
    try:
        load_info = pipeline.load()
        print(f"✓ pipeline.load() completed")
        if load_info is None:
            print("  ⚠️  WARNING: load_info is None - load() may have failed silently")
    except Exception as e:
        print(f"  ✗ ERROR in pipeline.load(): {e}")
        import traceback
        traceback.print_exc()
        raise
    
    # Log where data was actually loaded
    if load_info:
        print(f"✓ dlt load completed:")
        print(f"  Pipeline dataset_name: {pipeline.dataset_name}")
        print(f"  Expected dataset_name: {dataset_name}")
        if pipeline.dataset_name != dataset_name:
            print(f"  ⚠️  WARNING: Pipeline dataset name differs from expected!")
            print(f"     This means dlt appended a timestamp or suffix")
            print(f"     Tables are in: {pipeline.dataset_name}, not {dataset_name}")
        print(f"  Destination: {pipeline.destination.destination_name if hasattr(pipeline.destination, 'destination_name') else 'bigquery'}")
        # Get project from destination or environment
        project = os.environ.get('GCP_PROJECT', 'NOT SET')
        if hasattr(pipeline.destination, 'config'):
            if hasattr(pipeline.destination.config, 'project_id'):
                project = pipeline.destination.config.project_id
        print(f"  Project: {project}")
        # Get location from config_params (dlt stores it there)
        location = 'NOT SET'
        if hasattr(pipeline.destination, 'config_params') and isinstance(pipeline.destination.config_params, dict):
            location = pipeline.destination.config_params.get('location', 'NOT SET')
        elif hasattr(pipeline.destination, 'configuration'):
            # Fallback: try configuration if it's callable
            try:
                config = pipeline.destination.configuration()
                if hasattr(config, 'location'):
                    location = config.location
            except:
                pass
        print(f"  Location: {location}")
        # Log load info details - CRITICAL for debugging dataset creation
        if hasattr(load_info, 'jobs'):
            if load_info.jobs:
                print(f"  Jobs: {len(load_info.jobs)}")
                for i, job in enumerate(load_info.jobs[:3]):  # Show first 3 jobs
                    if hasattr(job, 'job_file_info'):
                        print(f"    Job {i+1}: {len(job.job_file_info)} files")
                    if hasattr(job, 'table_name'):
                        print(f"    Table: {job.table_name}")
                    # Log BigQuery job ID if available
                    if hasattr(job, 'job_id'):
                        print(f"    BigQuery Job ID: {job.job_id}")
                    # Check for errors in job
                    if hasattr(job, 'exception'):
                        print(f"    ⚠️  ERROR in job: {job.exception}")
            else:
                print(f"  ⚠️  WARNING: load_info.jobs is EMPTY - dlt did not create any load jobs!")
                print(f"     This means data was extracted but NOT loaded to BigQuery")
                print(f"     Check if there are pending packages or if load() failed silently")
        
        # Check for load errors
        if hasattr(load_info, 'loads'):
            for load in load_info.loads:
                if hasattr(load, 'exception'):
                    print(f"  ⚠️  LOAD ERROR: {load.exception}")
                if hasattr(load, 'job_info'):
                    job_info = load.job_info
                    if hasattr(job_info, 'job_id'):
                        print(f"  BigQuery Job ID: {job_info.job_id}")
                    if hasattr(job_info, 'exception'):
                        print(f"  ⚠️  JOB ERROR: {job_info.exception}")
        
        # Log full load_info structure for debugging
        try:
            load_dict = load_info.as_dict() if hasattr(load_info, 'as_dict') else str(load_info)
            if isinstance(load_dict, dict):
                if 'loads' in load_dict:
                    for load in load_dict['loads']:
                        if 'exception' in load:
                            print(f"  ⚠️  EXCEPTION in load: {load['exception']}")
                        if 'job_info' in load and isinstance(load['job_info'], dict):
                            if 'exception' in load['job_info']:
                                print(f"  ⚠️  EXCEPTION in job_info: {load['job_info']['exception']}")
        except:
            pass
    
    return dataset_name
