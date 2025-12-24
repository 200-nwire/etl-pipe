# Docker Compose Deployment Guide

This guide explains how to deploy Dagster using Docker Compose, both for local development and production deployments.

## Overview

The Docker Compose setup includes:

1. **PostgreSQL** - Database for Dagster run storage, schedule storage, and event log
2. **User Code Server** - gRPC server that loads your Dagster definitions
3. **Dagster Webserver** - Web UI for monitoring and triggering runs
4. **Dagster Daemon** - Background process for scheduling and running jobs

## Prerequisites

- Docker and Docker Compose installed
- Environment variables configured (see `.env.example`)
- GCP service account credentials file (if using BigQuery)

## Quick Start

### 1. Configure Environment Variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
# Edit .env with your actual values
```

Key variables:
- `DAGSTER_POSTGRES_*` - PostgreSQL connection details
- `MONGO_*` - MongoDB connection details
- `GCP_PROJECT`, `GOOGLE_APPLICATION_CREDENTIALS` - BigQuery configuration
- `BQ_LOCATION`, `BQ_DATASET_PREFIX` - BigQuery dataset settings
- `XAPI_LRS_ENDPOINT` - xAPI LRS endpoint

### 2. Mount GCP Credentials (Optional)

If you're using BigQuery, you need to make your GCP service account credentials available to the containers.

**Option A: Mount as volume** (recommended for local development)

Edit `docker-compose.yml` and uncomment/modify the volume mount in `dagster_user_code`:

```yaml
volumes:
  - /path/to/your/credentials.json:/tmp/gcp_credentials.json:ro
```

Then set in `.env`:
```bash
GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp_credentials.json
```

**Option B: Copy into image** (not recommended for production)

Add to `Dockerfile_user_code`:
```dockerfile
COPY path/to/credentials.json /tmp/gcp_credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/tmp/gcp_credentials.json
```

**Option C: Use environment variables** (for CI/CD)

Base64 encode your credentials and pass as environment variable.

### 3. Build and Start Services

```bash
# Build images
docker-compose build

# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Check service status
docker-compose ps
```

### 4. Access Dagster UI

Open your browser to:
- **Webserver**: http://localhost:3000

## Local Development

For local development, you can run Dagster directly without Docker Compose:

```bash
cd lineage
dg dev --port 3500
```

Or use the traditional CLI:
```bash
cd lineage
dagster dev --port 3500
```

The Docker Compose setup is useful for:
- Testing the full deployment stack
- Running in environments similar to production
- Isolating dependencies

## Production Considerations

### Security

1. **Credentials**: Never commit credentials to git. Use Docker secrets, mounted volumes, or environment variables from a secrets manager.

2. **PostgreSQL**: Use a managed PostgreSQL service (RDS, Cloud SQL) instead of the containerized version for production.

3. **Networks**: Use Docker networks to isolate services.

### Scaling

- **Webserver**: Can run multiple instances behind a load balancer
- **Daemon**: Run a single instance (or use leader election)
- **User Code**: Each code location can have its own container

### Monitoring

- Use `docker-compose logs` to view logs
- Set up log aggregation (e.g., ELK, CloudWatch)
- Monitor container health with Docker healthchecks

### Volumes

The setup uses named volumes for:
- `dagster_postgres_data` - PostgreSQL data persistence
- `dagster_io_storage` - Shared storage for IO manager

These persist data across container restarts. For production, consider using external volumes or cloud storage.

## Troubleshooting

### Services won't start

1. Check logs: `docker-compose logs <service_name>`
2. Verify environment variables are set correctly
3. Ensure ports 3000, 4000, 5432 are not in use
4. Check Docker daemon is running

### User code server not connecting

1. Verify `dagster_user_code` service is healthy: `docker-compose ps`
2. Check `workspace.yaml` has correct host/port
3. Verify network connectivity: `docker-compose exec dagster_webserver ping dagster_user_code`

### BigQuery connection issues

1. Verify credentials file is mounted correctly
2. Check `GOOGLE_APPLICATION_CREDENTIALS` path in container
3. Verify service account has required BigQuery permissions
4. Check `GCP_PROJECT` matches your project ID

### Asset materialization fails

1. Check user code logs: `docker-compose logs dagster_user_code`
2. Verify all environment variables are passed to run containers
3. Check network connectivity between services
4. Verify dependencies are installed in user code image

## File Structure

```
.
├── docker-compose.yml          # Docker Compose configuration
├── Dockerfile_dagster          # Image for webserver/daemon
├── Dockerfile_user_code        # Image for user code server
├── dagster.yaml                # Dagster instance configuration
├── workspace.yaml              # Code location configuration
├── .env.example                # Environment variable template
└── .env                        # Your actual environment variables (not in git)
```

## Differences from Local Development

| Aspect | Local (`dg dev`) | Docker Compose |
|--------|------------------|----------------|
| PostgreSQL | Optional (uses SQLite) | Required (containerized) |
| Code reload | Automatic | Requires rebuild |
| Ports | 3500 (webserver) | 3000 (webserver), 4000 (gRPC) |
| Isolation | Shared environment | Containerized |
| Scaling | Single process | Multiple containers |

## Next Steps

- Set up CI/CD to build and deploy images
- Configure monitoring and alerting
- Set up backup strategy for PostgreSQL
- Configure resource limits for containers
- Set up log rotation and retention

