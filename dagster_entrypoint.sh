#!/bin/bash
set -e

# Run configuration script to substitute environment variables in dagster.yaml
python3 /opt/dagster/dagster_home/configure_dagster.py

# Execute the original command
exec "$@"

