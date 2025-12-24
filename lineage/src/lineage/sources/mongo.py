"""dlt pipeline for MongoDB raw ingestion into BigQuery."""

# Update imports to use relative imports for the lineage package

import os
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import dlt
from bson import ObjectId
from pymongo import MongoClient

DEFAULT_MONGO_COLLECTIONS = [
    "users",
    "schools",
    "courses",
    "modules",
    "lessons",
    "pages",
    "blocks",
    "exercises",
    "exercise_submissions",
    "enrollments",
    "projects",
    "project_enrollments",
    "surveys",
    "survey_enrollments",
    "survey_submissions",
    "assessment_profiles",
    "rules"
]


def _get_mongo_client():
    """Create MongoDB client from environment variables."""
    # Try MONGO_URI first (for connection strings like mongodb+srv://)
    mongo_uri = os.environ.get("MONGO_URI")
    if mongo_uri:
        # Configure client for better replica set handling
        # Allow reads from secondary if primary is unavailable
        # Increase server selection timeout for replica sets
        return MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=30000,  # 30 seconds (default is 30s, but explicit)
            connectTimeoutMS=20000,  # 20 seconds
            socketTimeoutMS=20000,  # 20 seconds
            readPreference='primaryPreferred',  # Prefer primary, but allow secondary if primary is down
            retryWrites=True,
            retryReads=True,
        )
    
    # Fallback to individual components
    host = os.environ.get("MONGO_HOST")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    user = os.environ.get("MONGO_USER")
    password = os.environ.get("MONGO_PASSWORD")
    database = os.environ.get("MONGO_DATABASE")
    connection_string = os.environ.get("MONGO_CONNECTION_STRING")
    
    if connection_string:
        return MongoClient(connection_string)
    
    if not all([host, user, password, database]):
        raise RuntimeError(
            "MongoDB connection requires MONGO_URI, or MONGO_HOST, MONGO_USER, MONGO_PASSWORD, MONGO_DATABASE "
            "or MONGO_CONNECTION_STRING environment variables"
        )
    
    uri = f"mongodb://{user}:{password}@{host}:{port}/{database}"
    params = os.environ.get("MONGO_PARAMETERS", "")
    if params:
        uri += params if params.startswith("?") else f"?{params}"
    
    return MongoClient(uri)


def _convert_mongo_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert MongoDB document to JSON-serializable format.
    
    Converts:
    - ObjectId -> string
    - datetime -> ISO format string
    - date -> ISO format string
    - Recursively handles nested dicts and lists
    """
    if isinstance(doc, dict):
        return {
            key: _convert_mongo_doc(value)
            for key, value in doc.items()
        }
    elif isinstance(doc, list):
        return [_convert_mongo_doc(item) for item in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, (datetime, date)):
        return doc.isoformat()
    else:
        return doc


def load_mongo_raw(
    collections: Optional[List[str]] = None,
    *,
    destination: Optional[str] = None,
    dataset_name: str = "raw",
    pipeline_kwargs: Optional[dict] = None,
    source: Optional[dlt.sources.DltSource] = None,
) -> List[str]:
    """
    Load MongoDB collections into raw dataset using dlt.
    
    Supports both BigQuery and DuckDB destinations. Defaults to DuckDB if DLT_DESTINATION
    is set to 'duckdb', otherwise uses BigQuery.

    Connection details are sourced from environment variables:
    - MONGO_URI (preferred, full connection string)
    - Or MONGO_HOST, MONGO_USER, MONGO_PASSWORD, MONGO_DATABASE
    """

    # Determine destination from environment or parameter
    if destination is None:
        destination = os.environ.get("DLT_DESTINATION", "bigquery")
    
    selected_collections = collections or DEFAULT_MONGO_COLLECTIONS
    
    # Build pipeline args
    pipeline_kwargs = pipeline_kwargs or {}
    
    # For DuckDB, create destination instance with credentials containing database path
    # Configure for better concurrency handling
    if destination == "duckdb":
        # Use separate database file per collection to avoid lock conflicts
        # This allows parallel materialization and database exploration
        base_db_path = os.environ.get("DUCKDB_DATABASE", "/tmp/etl_duckdb.db")
        
        # If collections specified and single collection, use collection-specific DB
        # Otherwise use base path (for multi-collection loads)
        if collections and len(collections) == 1:
            collection_name = collections[0]
            # Use collection-specific database file
            db_dir = os.path.dirname(base_db_path) or "/tmp"
            db_name = os.path.basename(base_db_path).replace(".db", f"_{collection_name}.db")
            db_path = os.path.join(db_dir, db_name)
        else:
            db_path = base_db_path
        
        from dlt.destinations.impl.duckdb.factory import DuckDbCredentials
        from dlt.destinations.impl.duckdb.factory import duckdb as duckdb_factory
        # Configure DuckDB for better concurrency
        # Note: Only use valid DuckDB pragmas
        # Valid pragmas: memory_limit, threads (but syntax is different), checkpoint_threshold
        credentials = DuckDbCredentials(
            conn_or_path=db_path,
            # Remove invalid pragmas - DuckDB handles concurrency internally
            # If needed, configure via connection string or DuckDB settings
        )
        destination = duckdb_factory(credentials=credentials)
    
    # Use collection-specific pipeline name to avoid state file conflicts
    # when multiple assets run in parallel
    if collections and len(collections) == 1:
        pipeline_name = f"mongo_raw_{collections[0]}"
    else:
        pipeline_name = "mongo_raw"
    
    pipeline_args = {
        "pipeline_name": pipeline_name,
        "dataset_name": dataset_name,
        "full_refresh": False,
        "destination": destination,  # String for BigQuery, instance for DuckDB
        **(pipeline_kwargs or {}),
    }
    
    pipeline = dlt.pipeline(**pipeline_args)

    if source is not None:
        pipeline.run(source.with_resources(*selected_collections))
    else:
        # Create custom dlt source using pymongo
        client = _get_mongo_client()
        # Extract database name from URI or use env var
        mongo_uri = os.environ.get("MONGO_URI", "")
        db_name = os.environ.get("MONGO_DATABASE")
        
        if mongo_uri and not db_name:
            # Parse database from URI (mongodb+srv://user:pass@host/dbname)
            # URI format: mongodb+srv://user:pass@host/dbname?params
            try:
                # Split by / and get the last part before ?
                uri_parts = mongo_uri.split("/")
                if len(uri_parts) > 3:
                    # Has database name in path
                    potential_db = uri_parts[-1].split("?")[0].split("&")[0]
                    # Only use if it's not empty and not just query params
                    if potential_db and potential_db.strip():
                        db_name = potential_db.strip()
            except Exception:
                pass
        
        # Fallback to default if still not set or empty
        if not db_name or not db_name.strip():
            db_name = "default"
        
        db = client[db_name]
        
        # Get pipeline state for incremental loading
        # pipeline.state is a dict property, not a method
        state = pipeline.state
        
        # Track max modified timestamps per collection for state updates
        max_modified_by_collection = {}
        
        @dlt.source
        def mongo_source():
            for collection_name in selected_collections:
                # Prefix table name with lms_ for clarity
                table_name = f"lms_{collection_name}"
                
                # Get last modified timestamp from state for this collection
                collection_state = state.get("sources", {}).get(table_name, {})
                last_modified = collection_state.get("last_modified")
                
                # Capture variables in closure to avoid late binding issues
                coll_name = collection_name
                tbl_name = table_name
                last_mod = last_modified
                
                @dlt.resource(name=tbl_name)
                def collection_resource():
                    collection = db[coll_name]
                    
                    # Build query for incremental loading
                    # Most MongoDB collections have 'modified_on' or 'updatedAt' field
                    query = {}
                    if last_mod:
                        # Query for documents modified after last run
                        # Try common timestamp field names
                        query = {
                            "$or": [
                                {"modified_on": {"$gt": last_mod}},
                                {"updatedAt": {"$gt": last_mod}},
                                {"updated_at": {"$gt": last_mod}},
                                {"modifiedAt": {"$gt": last_mod}},
                            ]
                        }
                    
                    # Track max modified timestamp for this collection
                    max_modified = last_mod
                    
                    # Execute query with cursor batch size for efficient chunking
                    # MongoDB cursors fetch data in batches (default: 101 docs)
                    # Explicitly set batch_size for better control and memory efficiency
                    # Larger batch_size = fewer round trips, but more memory per batch
                    cursor = collection.find(query).batch_size(1000)  # Fetch 1000 docs per batch
                    
                    # Iterate over documents (cursor streams data in chunks)
                    for doc in cursor:
                        # Convert MongoDB types to JSON-serializable types
                        converted_doc = _convert_mongo_doc(doc)
                        
                        # Update max_modified from document
                        doc_modified = (
                            doc.get("modified_on") or
                            doc.get("updatedAt") or
                            doc.get("updated_at") or
                            doc.get("modifiedAt") or
                            doc.get("created_on") or
                            doc.get("createdAt")
                        )
                        if doc_modified:
                            if max_modified is None or doc_modified > max_modified:
                                max_modified = doc_modified
                        
                        yield converted_doc
                    
                    # Store max_modified for this collection to update state after run
                    if max_modified and max_modified != last_mod:
                        max_modified_by_collection[tbl_name] = max_modified
                
                yield collection_resource
        
        pipeline.run(mongo_source())
        
        # Update state with latest timestamps after successful run
        # Note: dlt automatically saves state after pipeline.run(), but we can update it
        if max_modified_by_collection:
            if "sources" not in state:
                state["sources"] = {}
            for table_name, max_modified in max_modified_by_collection.items():
                state["sources"][table_name] = {"last_modified": max_modified}
            # State is automatically saved by dlt after pipeline.run()
        
        client.close()
    
    return list(selected_collections)
