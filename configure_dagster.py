#!/usr/bin/env python3
"""Script to configure dagster.yaml with environment variable substitution."""
import os
import sys
from pathlib import Path

def configure_dagster_yaml():
    """Replace environment variable placeholders in dagster.yaml."""
    dagster_home = os.getenv("DAGSTER_HOME", "/opt/dagster/dagster_home")
    dagster_yaml_path = Path(dagster_home) / "dagster.yaml"
    
    if not dagster_yaml_path.exists():
        print(f"Warning: {dagster_yaml_path} does not exist", file=sys.stderr)
        return
    
    # Read the YAML file
    with open(dagster_yaml_path, "r") as f:
        content = f.read()
    
    # Get the credentials path from environment
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_HOST_PATH", "/dev/null")
    
    # Replace the placeholder with the actual path
    if "PLACEHOLDER_GOOGLE_APPLICATION_CREDENTIALS_HOST_PATH" in content:
        content = content.replace(
            "PLACEHOLDER_GOOGLE_APPLICATION_CREDENTIALS_HOST_PATH",
            creds_path
        )
        # Write back the modified content
        with open(dagster_yaml_path, "w") as f:
            f.write(content)
        print(f"Configured dagster.yaml with credentials path: {creds_path}")
    else:
        print("No placeholder found in dagster.yaml, skipping configuration", file=sys.stderr)

if __name__ == "__main__":
    configure_dagster_yaml()

