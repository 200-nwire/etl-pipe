"""Configurable resources for sources and destinations.

These resources can be configured from the Dagster UI without code changes.
Allows runtime configuration of:
- BigQuery dataset prefix
- LRS endpoint and authentication
- MongoDB connection details
"""

import os
from typing import Optional

from dagster import ConfigurableResource
from pydantic import Field


class BigQueryConfig(ConfigurableResource):
    """Configurable BigQuery destination settings.
    
    Can be configured from Dagster UI per-run or via environment variables.
    """
    project: str = Field(
        default_factory=lambda: os.environ.get("GCP_PROJECT", ""),
        description="GCP project ID for BigQuery"
    )
    dataset_prefix: str = Field(
        default_factory=lambda: os.environ.get("BQ_DATASET_PREFIX", "").strip(),
        description=(
            "Dataset prefix (e.g., 'dev_' for dev_raw, dev_staging, dev_silver). "
            "Empty string for no prefix."
        ),
    )
    location: str = Field(
        default_factory=lambda: os.environ.get("BQ_LOCATION", "me-west1"),
        description=(
            "BigQuery location/region (e.g., 'me-west1' for Tel Aviv, 'US' for US). "
            "Configurable via BQ_LOCATION env var."
        ),
    )
    
    def get_raw_dataset(self) -> str:
        """Get the raw dataset name with prefix."""
        prefix = self.dataset_prefix.strip()
        if prefix and not prefix.endswith("_"):
            prefix = prefix + "_"
        return f"{prefix}raw" if prefix else "raw"
    
    def get_staging_dataset(self) -> str:
        """Get the staging dataset name with prefix."""
        prefix = self.dataset_prefix.strip()
        if prefix and not prefix.endswith("_"):
            prefix = prefix + "_"
        return f"{prefix}staging" if prefix else "staging"
    
    def get_silver_dataset(self) -> str:
        """Get the silver dataset name with prefix."""
        prefix = self.dataset_prefix.strip()
        if prefix and not prefix.endswith("_"):
            prefix = prefix + "_"
        return f"{prefix}silver" if prefix else "silver"


class LRSConfig(ConfigurableResource):
    """Configurable xAPI LRS source settings.
    
    Can be configured from Dagster UI per-run or via environment variables.
    """
    endpoint: str = Field(
        default_factory=lambda: os.environ.get("XAPI_LRS_ENDPOINT", ""),
        description="xAPI LRS endpoint URL (e.g., https://lrs.example.com/xapi/statements)"
    )
    auth_token: Optional[str] = Field(
        default_factory=lambda: os.environ.get("XAPI_AUTH_TOKEN"),
        description="Authentication token (Basic Auth base64 or username:password)"
    )
    
    def get_base_endpoint(self) -> str:
        """Get normalized endpoint URL."""
        base_endpoint = self.endpoint.rstrip("/")
        if not base_endpoint.endswith("/statements"):
            base_endpoint = base_endpoint + "/statements"
        return base_endpoint


class MongoDBConfig(ConfigurableResource):
    """Configurable MongoDB source settings.
    
    Can be configured from Dagster UI per-run or via environment variables.
    """
    uri: Optional[str] = Field(
        default_factory=lambda: os.environ.get("MONGO_URI"),
        description="MongoDB connection URI (mongodb:// or mongodb+srv://)"
    )
    database: Optional[str] = Field(
        default_factory=lambda: os.environ.get("MONGO_DATABASE"),
        description="MongoDB database name (extracted from URI if not provided)"
    )
    host: Optional[str] = Field(
        default_factory=lambda: os.environ.get("MONGO_HOST"),
        description="MongoDB host (if not using URI)"
    )
    port: int = Field(
        default_factory=lambda: int(os.environ.get("MONGO_PORT", "27017")),
        description="MongoDB port (if not using URI)"
    )
    user: Optional[str] = Field(
        default_factory=lambda: os.environ.get("MONGO_USER"),
        description="MongoDB username (if not using URI)"
    )
    password: Optional[str] = Field(
        default_factory=lambda: os.environ.get("MONGO_PASSWORD"),
        description="MongoDB password (if not using URI)"
    )
    
    def get_connection_string(self) -> str:
        """Get MongoDB connection string from configured values."""
        if self.uri:
            return self.uri
        
        if not all([self.host, self.user, self.password]):
            raise ValueError(
                "MongoDB connection requires either MONGO_URI, "
                "or MONGO_HOST, MONGO_USER, MONGO_PASSWORD"
            )
        
        uri = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}"
        if self.database:
            uri += f"/{self.database}"
        return uri
    
    def get_database_name(self) -> str:
        """Get database name, extracting from URI if needed."""
        if self.database:
            return self.database
        
        if self.uri:
            try:
                uri_parts = self.uri.split("/")
                if len(uri_parts) > 3:
                    potential_db = uri_parts[-1].split("?")[0].split("&")[0]
                    if potential_db and potential_db.strip():
                        return potential_db.strip()
            except Exception:
                pass
        
        return "default"


