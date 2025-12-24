# Lineage Project

Dagster project for orchestrating ETL pipelines with dlt and dbt.

## Structure

- `src/lineage/sources/` - dlt pipeline sources (MongoDB, xAPI)
- `src/lineage/defs/` - Dagster asset definitions
  - `sources.py` - External source assets
  - `raw_tables.py` - Raw ingestion assets
  - `dbt_assets.py` - dbt silver layer assets
  - `validation.py` - Data quality validation assets
- `src/lineage/dbt_translator.py` - Custom dbt translator for lineage mapping

## Setup

1. Install dependencies:
   ```bash
   cd lineage
   uv sync  # or pip install -e .
   ```

2. Set environment variables (see parent `.env` file):
   - `MONGO_URI` or MongoDB connection details
   - `XAPI_LRS_ENDPOINT` and `XAPI_AUTH_TOKEN`
   - `DBT_TARGET=duckdb` (for local dev)
   - `DAGSTER_HOME=$PWD/../.dagster_home`

3. Run Dagster:
   ```bash
   dg dev --port 3500
   ```

## Assets

- **Sources**: MongoDB LMS, xAPI LRS
- **Raw**: 29 MongoDB collections + xAPI statements
- **Silver**: dbt models (dimensions, facts, reference tables)
- **Validation**: dbt test suite

