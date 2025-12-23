"""dlt pipeline for ingesting xAPI LRS statements into BigQuery."""

# Update imports to use relative imports for the lineage package

from __future__ import annotations

import os
from typing import Dict, Iterable

import dlt
import requests

XAPI_PAGE_SIZE = 500


def _fetch_xapi_statements(since: str | None = None) -> Iterable[Dict]:
    """Simple generator fetching paginated xAPI statements using the LRS REST API.
    
    Follows xAPI LRS specification:
    - Uses Basic Authentication
    - Includes required X-Experience-API-Version header
    - Handles pagination via 'more' URL in response
    - Supports query parameters: limit, since, until, etc.
    
    Args:
        since: ISO 8601 timestamp (e.g., "2025-12-01T00:00:00Z") to fetch only statements
               stored after this time. If None, fetches all statements.
    """

    endpoint = os.environ.get("XAPI_LRS_ENDPOINT")
    auth_token = os.environ.get("XAPI_AUTH_TOKEN")
    if not endpoint:
        raise RuntimeError("XAPI_LRS_ENDPOINT is required for xAPI ingestion")

    # Store base endpoint for pagination
    base_endpoint = endpoint.rstrip("/")
    # Ensure endpoint ends with /statements if not already
    if not base_endpoint.endswith("/statements"):
        endpoint = base_endpoint + "/statements"
    else:
        endpoint = base_endpoint

    # xAPI LRS requires X-Experience-API-Version header (typically 1.0.3)
    headers = {
        "X-Experience-API-Version": "1.0.3",
        "Content-Type": "application/json",
    }
    
    # Add Basic Auth if token provided
    if auth_token:
        headers["Authorization"] = f"Basic {auth_token}"
    
    # xAPI LRS query parameters
    # Use 'since' parameter for incremental loading (xAPI LRS spec)
    params: Dict[str, str | int] = {
        "limit": XAPI_PAGE_SIZE,
    }
    if since:
        # xAPI LRS 'since' parameter filters by stored timestamp
        params["since"] = since
    
    more = True
    page_count = 0
    max_pages = 10000  # Safety limit to prevent infinite loops
    
    while more and page_count < max_pages:
        try:
            response = requests.get(
                endpoint,
                headers=headers,
                params=params,
                timeout=60,  # Increased timeout for large responses
            )
            response.raise_for_status()
            
            # xAPI LRS returns JSON with 'statements' array
            payload = response.json()
            
            # Handle both response formats:
            # 1. Direct array of statements: [{"id": "...", ...}, ...]
            # 2. Object with statements array: {"statements": [...], "more": "..."}
            if isinstance(payload, list):
                statements = payload
                more_url = None
            else:
                statements = payload.get("statements", [])
                more_url = payload.get("more")
            
            # Yield each statement
            for stmt in statements:
                yield stmt
            
            # Handle pagination
            if more_url:
                # 'more' can be a full URL or a relative path
                if more_url.startswith("http://") or more_url.startswith("https://"):
                    endpoint = more_url
                else:
                    # Relative path - construct from base endpoint
                    # Use the stored base_endpoint to avoid duplication
                    if more_url.startswith("/"):
                        # Absolute path from root
                        endpoint = f"{base_endpoint}{more_url}"
                    elif more_url.startswith("?"):
                        # Query parameters only
                        endpoint = f"{base_endpoint}/statements{more_url}"
                    else:
                        # Relative path
                        endpoint = f"{base_endpoint}/statements/{more_url}"
                # Clear params for subsequent requests (pagination URL may include them)
                params = {}
            else:
                more = False
            
            page_count += 1
            
        except requests.exceptions.HTTPError as e:
            # Provide more context for 400 errors
            if e.response.status_code == 400:
                error_detail = ""
                try:
                    error_body = e.response.json()
                    error_detail = f" - {error_body}"
                except:
                    error_detail = f" - {e.response.text[:200]}"
                raise RuntimeError(
                    f"xAPI LRS returned 400 Bad Request for endpoint {endpoint}. "
                    f"This usually means the request format doesn't match the LRS specification.{error_detail}\n"
                    f"Check: endpoint URL, authentication, headers, and query parameters."
                ) from e
            raise


def load_xapi_raw(
    *,
    destination: str | None = None,
    dataset_name: str = "raw",
    pipeline_kwargs: Dict | None = None,
    fetcher: Iterable[Dict] | None = None,
    since: str | None = None,
) -> str:
    """Load xAPI statements into the raw dataset using dlt.
    
    Supports both BigQuery and DuckDB destinations. Defaults to DuckDB if DLT_DESTINATION
    is set to 'duckdb', otherwise uses BigQuery.
    
    Args:
        since: ISO 8601 timestamp for incremental loading. If None, uses dlt state to
               track last successful run timestamp automatically.
    """

    # Determine destination from environment or parameter
    if destination is None:
        destination = os.environ.get("DLT_DESTINATION", "bigquery")
    
    # Build pipeline args
    pipeline_kwargs = pipeline_kwargs or {}
    
    # For DuckDB, create destination instance with credentials containing database path
    # Configure for better concurrency handling
    if destination == "duckdb":
        # Use separate database file for xAPI to avoid lock conflicts
        base_db_path = os.environ.get("DUCKDB_DATABASE", "/tmp/etl_duckdb.db")
        db_dir = os.path.dirname(base_db_path) or "/tmp"
        db_name = os.path.basename(base_db_path).replace(".db", "_xapi.db")
        db_path = os.path.join(db_dir, db_name)
        
        from dlt.destinations.impl.duckdb.factory import duckdb as duckdb_factory, DuckDbCredentials
        # Configure DuckDB for better concurrency
        # Note: Only use valid DuckDB pragmas
        # Valid pragmas: memory_limit, threads (but syntax is different), checkpoint_threshold
        credentials = DuckDbCredentials(
            conn_or_path=db_path,
            # Remove invalid pragmas - DuckDB handles concurrency internally
            # If needed, configure via connection string or DuckDB settings
        )
        destination = duckdb_factory(credentials=credentials)
    
    pipeline_args = {
        "pipeline_name": "xapi_raw",
        "dataset_name": dataset_name,
        "full_refresh": False,
        "destination": destination,  # String for BigQuery, instance for DuckDB
        **(pipeline_kwargs or {}),
    }
    
    pipeline = dlt.pipeline(**pipeline_args)
    
    # Use dlt state to track last successful timestamp for incremental loading
    # State is automatically persisted and restored between runs
    # pipeline.state is a dict property, not a method
    state = pipeline.state
    last_timestamp = since or state.get("sources", {}).get("xapi_statements", {}).get("last_timestamp")
    
    # Track max timestamp from this run for next incremental load
    max_timestamp = None

    @dlt.resource(name="xapi_statements")
    def xapi_resource():
        nonlocal max_timestamp
        for stmt in (fetcher if fetcher is not None else _fetch_xapi_statements(since=last_timestamp)):
            # Track the maximum stored timestamp for incremental loading
            # xAPI statements have 'stored' field (ISO 8601 timestamp)
            stored = stmt.get("stored") or stmt.get("timestamp")
            if stored:
                if max_timestamp is None or stored > max_timestamp:
                    max_timestamp = stored
            yield stmt

    pipeline.run(xapi_resource())
    
    # Update state with latest timestamp for next incremental run
    # Note: dlt automatically saves state after pipeline.run(), but we can update it
    if max_timestamp:
        if "sources" not in state:
            state["sources"] = {}
        state["sources"]["xapi_statements"] = {"last_timestamp": max_timestamp}
        # State is automatically saved by dlt after pipeline.run()
    
    return dataset_name
