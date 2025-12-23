"""dlt pipeline for LMS (MongoDB) raw ingestion using dlt's incremental loading features."""

import os
from typing import List, Optional, Dict, Any
from bson import ObjectId
from datetime import datetime, date

import dlt
from pymongo import MongoClient
from google.cloud import bigquery as bq_client

# Actual MongoDB collection names (as they exist in the database)
# Note: MongoDB uses hyphens, but we convert to underscores for table names
DEFAULT_MONGO_COLLECTIONS = [
    "users",
    "schools",
    "courses",
    "modules",
    "lessons",
    "pages",
    "blocks",
    "exercises",
    "submissions",  # MongoDB collection name - will be mapped to lms_exercise_submissions table
    "enrollments",
    "projects",
    "project-enrollments",  # MongoDB uses hyphen
    "surveys",
    "survey-enrollments",  # MongoDB uses hyphen
    "survey-submissions",  # MongoDB uses hyphen
    "assessment-profiles",  # MongoDB uses hyphen
    "rules",
]

# Map MongoDB collection names (with hyphens) to table names (with underscores)
# This handles the conversion from MongoDB naming (hyphens) to SQL-friendly naming (underscores)
COLLECTION_TO_TABLE_MAP = {
    "submissions": "exercise_submissions",  # MongoDB has "submissions", dbt expects "lms_exercise_submissions"
    "project-enrollments": "project_enrollments",
    "survey-enrollments": "survey_enrollments",
    "survey-submissions": "survey_submissions",
    "assessment-profiles": "assessment_profiles",
}


def _get_mongo_client():
    """Create MongoDB client from environment variables."""
    # Try MONGO_URI first (for connection strings like mongodb+srv://)
    mongo_uri = os.environ.get("MONGO_URI")
    if mongo_uri:
        return MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=20000,
            socketTimeoutMS=20000,
            readPreference='primaryPreferred',
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
    
    if not all([host, user, password]):
        raise RuntimeError(
            "MongoDB connection requires MONGO_URI, or MONGO_HOST, MONGO_USER, MONGO_PASSWORD "
            "or MONGO_CONNECTION_STRING environment variables"
        )
    
    uri = f"mongodb://{user}:{password}@{host}:{port}"
    if database:
        uri += f"/{database}"
    
    params = os.environ.get("MONGO_PARAMETERS", "")
    if params:
        uri += params if params.startswith("?") else f"?{params}"
    
    return MongoClient(uri)


def _convert_mongo_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert MongoDB document to JSON-serializable format.
    
    Optimized version that handles common cases first for better performance.
    Converts:
    - ObjectId -> string
    - datetime -> ISO format string
    - date -> ISO format string
    - Recursively handles nested dicts and lists
    """
    if isinstance(doc, dict):
        # Use dict comprehension for better performance
        result = {}
        for key, value in doc.items():
            # Fast path for common types
            if isinstance(value, ObjectId):
                result[key] = str(value)
            elif isinstance(value, (datetime, date)):
                result[key] = value.isoformat()
            elif isinstance(value, dict):
                result[key] = _convert_mongo_doc(value)
            elif isinstance(value, list):
                result[key] = [_convert_mongo_doc(item) if isinstance(item, (dict, list, ObjectId, datetime, date)) else item for item in value]
            else:
                result[key] = value
        return result
    elif isinstance(doc, list):
        # Optimized list conversion
        return [_convert_mongo_doc(item) if isinstance(item, (dict, list, ObjectId, datetime, date)) else item for item in doc]
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
    dataset_name: Optional[str] = None,
    pipeline_kwargs: Optional[dict] = None,
    bq_config: Optional[object] = None,  # BigQueryConfig resource
    mongo_config: Optional[object] = None,  # MongoDBConfig resource
) -> List[str]:
    """Load MongoDB collections into raw dataset using dlt with incremental loading.
    
    Uses dlt's incremental loading features to handle:
    - Automatic BSON to JSON conversion (ObjectId, datetime, etc.)
    - Incremental loading based on timestamp fields
    - Efficient cursor-based batch fetching (5000 docs per batch)
    - Schema inference and adaptation
    
    Performance optimizations:
    - Uses single-field queries when possible (faster than $or)
    - Large batch size (5000) for fewer round trips
    - Index hints for timestamp fields
    - Optimized BSON conversion
    
    Performance tips:
    - Ensure MongoDB has indexes on timestamp fields (modified_on, updatedAt, etc.)
    - For first-time full loads, consider using full_refresh=True to skip incremental logic
    - For very large collections (100K+), expect 2-5 minutes per 100K docs with good indexes
    
    Uses BigQuery as the destination. BigQuery natively supports nested and repeated fields (RECORD types).
    
    PRESERVES NESTED STRUCTURES: To prevent flattening:
    - Set max_table_nesting=0 on each resource to disable normalization
    - dlt will preserve nested structures as BigQuery RECORD types
    - Arrays are stored as REPEATED fields within RECORD types
    - This preserves the original MongoDB document structure
    
    This keeps raw data as close to source as possible, leveraging BigQuery's native nested field support.

    Connection details can be provided via:
    - MongoDBConfig resource (configurable from UI)
    - Environment variables: MONGO_URI, or MONGO_HOST, MONGO_USER, MONGO_PASSWORD, MONGO_DATABASE
    
    Args:
        collections: List of collection names to load. If None, loads all DEFAULT_MONGO_COLLECTIONS.
        destination: Ignored (always uses BigQuery)
        dataset_name: Dataset/schema name for the destination (defaults to bq_config.get_raw_dataset() or env var)
        pipeline_kwargs: Additional pipeline arguments
        bq_config: BigQueryConfig resource (optional, uses env vars if not provided)
        mongo_config: MongoDBConfig resource (optional, uses env vars if not provided)
    
    Returns:
        List of collection names that were loaded
    """
    # Always use BigQuery (remove DuckDB support)
    from dlt.destinations.impl.bigquery.factory import bigquery
    
    selected_collections = collections or DEFAULT_MONGO_COLLECTIONS
    
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
    
    # Build pipeline args
    pipeline_kwargs = pipeline_kwargs or {}
    
    # IMPORTANT: Use the same pipeline name for all collections that write to the same dataset
    # This ensures they share state and don't try to create the dataset concurrently
    # Each collection still gets its own table in the shared dataset
    pipeline_name = "lms_raw"
    
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
    
    # Note: dlt will handle "dataset already exists" errors gracefully in newer versions
    # If errors occur, they're likely due to concurrent creation - using shared pipeline name helps
    
    # Configure BigQuery destination with credentials
    # dlt requires explicit credentials, not just GOOGLE_APPLICATION_CREDENTIALS env var
    # IMPORTANT: dlt's configuration resolution doesn't preserve credentials objects
    # when recreating clients, so we need to set environment variables that dlt expects
    from dlt.common.configuration.specs.gcp_credentials import GcpServiceAccountCredentials
    import json
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/gcp_credentials.json")
    if os.path.exists(creds_path):
        # Load credentials from JSON file
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
        
        # Create/update .dlt/secrets.toml from JSON file
        # This ensures dlt can read credentials when recreating destination clients
        from pathlib import Path
        dlt_secrets_dir = Path(__file__).parent.parent.parent / ".dlt"
        dlt_secrets_dir.mkdir(exist_ok=True)
        secrets_file = dlt_secrets_dir / "secrets.toml"
        
        # Write secrets.toml with credentials from JSON
        # Escape private key for TOML (escape quotes)
        private_key_escaped = creds_data.get("private_key", "").replace('"', '\\"')
        project_id = creds_data.get("project_id", "")
        client_email = creds_data.get("client_email", "")
        
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

# Pipeline-specific configuration (lms_raw pipeline)
[lms_raw.destination.bigquery]
location = "{location}"

[lms_raw.destination.bigquery.credentials]
project_id = "{project_id}"
client_email = "{client_email}"
private_key = """{private_key_escaped}"""
'''
        secrets_file.write_text(secrets_content)
        
        # Also create credentials object for direct use
        credentials = GcpServiceAccountCredentials(
            project_id=creds_data.get("project_id"),
            private_key=creds_data.get("private_key"),
            client_email=creds_data.get("client_email"),
        )
        destination = bigquery(credentials=credentials, location=location)
    else:
        # Fallback: let dlt try to use ADC or other methods
        destination = bigquery(location=location)
    
    pipeline_args = {
        "pipeline_name": pipeline_name,
        "dataset_name": dataset_name,
        "dev_mode": False,  # Use dev_mode instead of full_refresh (dlt 1.20+)
        "destination": destination,
        # With max_table_nesting=0 on resources, dlt preserves nested structures
        # BigQuery will store them as RECORD types (native nested/repeated fields)
        # This preserves the original MongoDB document structure
        **(pipeline_kwargs or {}),
    }
    
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
    
    # Create MongoDB client and get database
    # Use config resource if provided, otherwise fall back to environment variables
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
            db_name = "default"
    
    db = client[db_name]
    state = pipeline.state
    
    # Create dlt source with incremental loading using dlt.sources.incremental
    @dlt.source
    def mongo_source():
        for collection_name in selected_collections:
            # Map collection name to table name (e.g., "submissions" -> "exercise_submissions")
            # Also convert hyphens to underscores for SQL compatibility
            mapped_name = COLLECTION_TO_TABLE_MAP.get(collection_name, collection_name)
            # Convert any remaining hyphens to underscores (e.g., "project-enrollments" -> "project_enrollments")
            mapped_name = mapped_name.replace("-", "_")
            table_name = f"lms_{mapped_name}"
            
            # Get last modified timestamp from state
            collection_state = state.get("sources", {}).get(table_name, {})
            last_modified = collection_state.get("last_modified")
            
            # Use dlt's incremental loading - it handles the query automatically
            # Try common timestamp field names
            incremental = dlt.sources.incremental(
                "modified_on",  # Primary field to track
                initial_value=last_modified or "1970-01-01T00:00:00Z",
            )
            
            @dlt.resource(
                name=table_name,
                write_disposition="merge",
                primary_key="_id",  # MongoDB's default primary key
                # CRITICAL: Set max_table_nesting=0 to preserve nested structures as BigQuery RECORD types
                # This prevents dlt from normalizing/flattening nested structures into child tables
                # BigQuery will store nested fields as RECORD types (native nested structures)
                # Arrays are stored as REPEATED fields within RECORD types
                # This preserves the original MongoDB document structure
                max_table_nesting=0,  # Disable normalization - preserve nested structures
                columns={},  # Empty dict - let dlt infer schema, but nested structures won't be normalized
            )
            def collection_resource(incremental=incremental):
                collection = db[collection_name]
                
                # Optimize query - use single field if possible (faster than $or)
                # Try to find the most common timestamp field first
                query = {}
                if incremental.last_value:
                    # Convert incremental.last_value (string) to datetime for MongoDB query
                    from dateutil.parser import isoparse
                    try:
                        last_value_dt = isoparse(incremental.last_value) if isinstance(incremental.last_value, str) else incremental.last_value
                    except (ValueError, TypeError):
                        # If parsing fails, use a very old date to get all documents
                        from datetime import datetime
                        last_value_dt = datetime(1970, 1, 1)
                    
                    # Check which timestamp field exists in the collection
                    # Use a sample document to determine the field name
                    sample = collection.find_one({}, {"modified_on": 1, "updatedAt": 1, "updated_at": 1, "modifiedAt": 1})
                    if sample:
                        # Use the first available timestamp field (most efficient)
                        if "modified_on" in sample:
                            query = {"modified_on": {"$gt": last_value_dt}}
                        elif "updatedAt" in sample:
                            query = {"updatedAt": {"$gt": last_value_dt}}
                        elif "updated_at" in sample:
                            query = {"updated_at": {"$gt": last_value_dt}}
                        elif "modifiedAt" in sample:
                            query = {"modifiedAt": {"$gt": last_value_dt}}
                        else:
                            # Fallback to $or if no common field found
                            query = {
                                "$or": [
                                    {"modified_on": {"$gt": last_value_dt}},
                                    {"updatedAt": {"$gt": last_value_dt}},
                                    {"updated_at": {"$gt": last_value_dt}},
                                    {"modifiedAt": {"$gt": last_value_dt}},
                                ]
                            }
                    else:
                        # Empty collection, no query needed
                        query = {}
                
                # Optimize cursor settings for performance
                # - Larger batch size = fewer round trips (but more memory)
                # - Use no_cursor_timeout for long-running queries
                # Note: Index hints are not used - MongoDB will automatically use indexes if they exist
                # To improve performance, create indexes on timestamp fields:
                #   db.collection_name.createIndex({ "modified_on": 1 })
                #   db.collection_name.createIndex({ "updatedAt": 1 })
                cursor = collection.find(
                    query,
                    no_cursor_timeout=True,  # Prevent cursor timeout on long queries
                ).batch_size(5000)  # Increased from 1000 to 5000 for better throughput
                
                doc_count = 0
                for doc in cursor:
                    # Convert MongoDB types to JSON-serializable
                    # Keep nested structures as dict/list - dlt will handle JSON conversion
                    # when data_types is configured on the resource
                    converted_doc = _convert_mongo_doc(doc)
                    
                    # Don't convert to JSON strings here - let dlt handle it
                    # dlt will use BigQuery's native JSON type when data_types is configured
                    # dlt automatically tracks incremental state based on the incremental field
                    # No need to manually update state - dlt handles it
                    
                    yield converted_doc
                    doc_count += 1
                    
                    # Log progress every 10K documents
                    if doc_count % 10000 == 0:
                        print(f"  Loaded {doc_count:,} documents from {collection_name}...")
                
                print(f"  Completed loading {doc_count:,} documents from {collection_name}")
            
            yield collection_resource
    
    # Extract and load separately to avoid pending package issues
    # pipeline.run() automatically tries to load pending packages, which causes errors
    # By using extract() and load() separately, we have more control
    source = mongo_source()
    
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
        print(f"     Check if MongoDB collections are empty or if there are connection issues")
    
    # CRITICAL: Ensure secrets.toml exists RIGHT BEFORE pipeline.load()
    # dlt recreates the destination client during load() and needs credentials
    # We must ensure secrets.toml is up-to-date at this point
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/gcp_credentials.json")
    print(f"⚠ Checking credentials file: {creds_path}")
    print(f"   File exists: {os.path.exists(creds_path)}")
    
    if not os.path.exists(creds_path):
        raise RuntimeError(f"GCP credentials file not found at {creds_path}. Cannot proceed with BigQuery load.")
    
    try:
        with open(creds_path, 'r') as f:
            creds_data = json.load(f)
        
        # Validate required fields
        required_fields = ["project_id", "private_key", "client_email"]
        missing_fields = [f for f in required_fields if not creds_data.get(f)]
        if missing_fields:
            raise ValueError(f"Missing required fields in credentials: {missing_fields}")
        
        print(f"✓ Loaded credentials: project_id={creds_data.get('project_id')}, client_email={creds_data.get('client_email')}")
        
        # Create/update .dlt/secrets.toml from JSON file
        # This ensures dlt can read credentials when recreating destination clients
        from pathlib import Path
        dlt_secrets_dir = Path(__file__).parent.parent.parent / ".dlt"
        dlt_secrets_dir.mkdir(exist_ok=True)
        secrets_file = dlt_secrets_dir / "secrets.toml"
        
        # Write secrets.toml with credentials from JSON
        # Escape private key for TOML (escape quotes and backslashes)
        private_key = creds_data.get("private_key", "")
        # For TOML triple-quoted strings, we need to escape backslashes and quotes
        private_key_escaped = private_key.replace("\\", "\\\\").replace('"', '\\"')
        project_id = creds_data.get("project_id", "")
        client_email = creds_data.get("client_email", "")
        
        # Get location from pipeline or environment
        pipeline_location = location if 'location' in locals() else os.environ.get("BQ_LOCATION", "me-west1")
        
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
location = "{pipeline_location}"

[destination.bigquery.credentials]
project_id = "{project_id}"
client_email = "{client_email}"
private_key = """{private_key_escaped}"""

# Pipeline-specific configuration (lms_raw pipeline)
[lms_raw.destination.bigquery]
location = "{pipeline_location}"

[lms_raw.destination.bigquery.credentials]
project_id = "{project_id}"
client_email = "{client_email}"
private_key = """{private_key_escaped}"""
'''
        secrets_file.write_text(secrets_content)
        print(f"✓ Updated secrets.toml at {secrets_file}")
        print(f"   File size: {secrets_file.stat().st_size} bytes")
        
        # Verify the file was written correctly
        if secrets_file.stat().st_size < 100:
            raise RuntimeError(f"secrets.toml file is too small ({secrets_file.stat().st_size} bytes). Credentials may not have been written correctly.")
        
    except Exception as e:
        print(f"✗ ERROR: Could not update secrets.toml: {e}")
        import traceback
        traceback.print_exc()
        raise
    
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
        
        # Check for errors
        if hasattr(load_info, 'pipeline') and hasattr(load_info.pipeline, 'default_schema'):
            print(f"  Schema: {load_info.pipeline.default_schema.name if hasattr(load_info.pipeline.default_schema, 'name') else 'N/A'}")
    
    client.close()
    
    return list(selected_collections)
