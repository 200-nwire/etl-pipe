# Testing strategy

This repository uses three complementary layers of testing to cover Dagster orchestration, dlt ingestion, and dbt modeling while keeping CI fast and credentials-light.

## 1) Static analysis and import safety
- **Ruff** enforces formatting and cleanliness on the Dagster, dlt, dbt-adjacent Python code.
- **`python -m compileall`** runs during CI to catch syntax errors across Dagster assets, pipelines, and tests without importing any external services.

## 2) Targeted unit checks for Dagster definitions
- `tests/test_imports.py` imports the Dagster `Definitions` to ensure the asset graph is loadable. This guards against resource/asset registration drift without requiring a running Dagster instance or external secrets.
- For deeper Dagster coverage, add `materialize_to_memory`-style tests that monkeypatch the dlt loaders to inject fixtures, then run individual assets in-memory. This pattern avoids hitting networked sources while still validating Dagster IO managers, asset metadata, and deps.

## 3) End-to-end ingestion and modeling with DuckDB
- `tests/test_duckdb_e2e.py` uses the real dlt loaders for MongoDB and xAPI but swaps the destination to **DuckDB**. This exercises extract/transform/load logic and dbt schemas without provisioning BigQuery or live data.
- The sample fixtures mimic production payloads and run through dbt models via DuckDB-backed sources so seed/schema tests can execute in CI.
- This approach is a widely recommended compromise for data platform repos: use production-like logic with a local/ephemeral destination instead of heavy mocks.
- To run these tests rather than skip them, install the full dev extras (`pip install .[dev]`) which include dlt's DuckDB/Mongo extras and the DuckDB engine itself; otherwise pytest will emit a clear skip reason.

## CI considerations
- The GitHub Actions workflow invokes Ruff, `compileall`, `dbt parse`, `dbt build` (targeting DuckDB), and the Python test suite (including Dagster asset tests and DuckDB end-to-end checks). No BigQuery credentials are required; parse uses placeholder env vars and a temporary JSON key at `/tmp/fake.json`.
- To run Dagster-involved tests in CI, set `DAGSTER_HOME` to a temp directory and prefer in-memory materializations with patched loaders to keep runs hermetic.

## Local developer workflow
- Use DuckDB to iterate quickly on pipeline changes: `pytest tests/test_duckdb_e2e.py`.
- When you need full Dagster orchestration, start `dagster dev -m dagster_project` and trigger the ingestion assets; the same dlt loaders are used, so behavior matches what the tests cover.
- For BigQuery validation, point the dlt loaders and dbt profiles at a sandbox dataset, then run `dbt run`/`dbt test` as usual.
