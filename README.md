# etl-pipe

Modern ELT reference stack that ingests MongoDB (Airbyte) and xAPI LRS data with [dlt](https://dlthub.com/), orchestrates with [Dagster](https://dagster.io/), and models into a BigQuery silver layer with [dbt](https://www.getdbt.com/).

## Repository structure

- `lineage/` – Dagster project with all assets, sources, and orchestration (main project).
- `dbt/` – dbt project implementing raw sources, staging views, and silver models aligned to the provided schema.
- `.github/workflows/ci.yml` – CI pipeline for linting, Dagster asset checks, and dbt parsing/testing.
- `docs/` – architecture, lineage, and operational documentation.
 - `docs/` – architecture, lineage, operational guidance, and testing strategy.

## Quickstart

1. Install dependencies (preferably in a virtual environment):
   ```bash
   pip install -e .[dev]
   ```
2. Export secrets (see `docs/architecture.md`):
   ```bash
   export MONGO_USER=... MONGO_PASSWORD=... MONGO_DATABASE=... MONGO_HOST=...
   export GCP_PROJECT=... GCP_BQ_DATASET=raw GOOGLE_APPLICATION_CREDENTIALS=/path/key.json
   export XAPI_LRS_ENDPOINT=https://lrs.example.com/xapi/statements
   export XAPI_AUTH_TOKEN=...  # optional
   export DBT_PROFILES_DIR=$PWD/dbt
   ```
3. Set Dagster home (optional but recommended to avoid temp directories):
   ```bash
   export DAGSTER_HOME=$PWD/.dagster_home
   ```

4. Run Dagster locally (using `dg` CLI - recommended):
   ```bash
   cd lineage && dg dev --port 3500
   ```
   
   Or using traditional CLI:
   ```bash
   cd lineage && dagster dev --port 3500
   ```
5. Trigger ingestion and transformations from Dagster UI or CLI:
   ```bash
  # Materialize assets via Dagster UI or:
  dagster asset materialize -m lineage -s lineage.definitions -a raw__lms_users
  dbt run --project-dir dbt --profiles-dir dbt
  dbt test --project-dir dbt --profiles-dir dbt
  ```

### MongoDB Atlas readiness

- The dlt Mongo connector works with standard SRV connection hosts from Atlas. Set
  `MONGO_HOST` to your Atlas cluster hostname (e.g. `cluster0.abcd123.mongodb.net`),
  `MONGO_PORT=27017`, and include TLS/write concern flags via
  `MONGO_PARAMETERS='?retryWrites=true&w=majority&tls=true'`. Provide username,
  password, and database via `MONGO_USER`, `MONGO_PASSWORD`, and `MONGO_DATABASE`.
- If you prefer a single SRV URL, define `MONGO_URI` in your environment;
  the Dagster asset will pass it through to the connector unchanged.

### Local validation without BigQuery

- The dlt pipelines accept a `destination="duckdb"` override so you can run integration tests
  without provisioning BigQuery.
- A pytest suite (`tests/test_duckdb_e2e.py`) exercises Mongo and xAPI ingestion end-to-end
  into DuckDB. Run it locally or in CI with:
  ```bash
  pytest tests/test_duckdb_e2e.py
  ```

For more on the testing approach (Dagster unit patterns, DuckDB E2E design, and CI setup), see `docs/testing.md`.

## CI/CD and deployment

- GitHub Actions workflow (`ci.yml`) lints Python via Ruff, parses dbt models, and runs unit tests (including the DuckDB
  end-to-end suite) on every push and PR. No BigQuery credentials are required; dbt parse uses a throwaway service account
  JSON (`GOOGLE_APPLICATION_CREDENTIALS=/tmp/fake.json`) plus `GCP_PROJECT`/`GCP_BQ_DATASET` placeholders.
- Containerize with the provided Dockerfile for K8s or ECS. The container bundles Dagster webserver and dbt CLI.
- Use Dagster's `dagster-cloud` or self-hosted `dagster-daemon` for scheduling and sensor-based ingestion in production.

## Observability and validation

- Dagster assets expose metadata, lineage, and quality status for ingestion and dbt tests.
- dbt `schema.yml` files define primary key uniqueness/not-null tests across all silver models.
- dlt automatically tracks load package state; jobs are idempotent and resumable.

Refer to `docs/architecture.md` and `docs/lineage.md` for detailed lineage diagrams, icons, and operational guidance.

## AI Assistant Setup (MCP)

To enable better AI assistance in Cursor or other MCP-compatible tools, set up MCP servers for Dagster, dbt, and dlt. This allows AI assistants to understand your project structure, execute commands, and generate code with proper context.

**Quick Setup:**
1. See `docs/MCP_SETUP.md` for complete instructions
2. Install MCP dependencies: `pip install uv` (for dbt MCP)
3. Configure Cursor: Copy `.cursor/mcp.json.example` to `.cursor/mcp.json` and customize paths
4. Restart Cursor to enable MCP integration

**Benefits:**
- AI can understand your Dagster asset graph and dependencies
- AI can query dbt models, lineage, and execute dbt commands
- AI can help generate new assets/models with proper context
- Better debugging and troubleshooting assistance
