FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install uv for fast dependency management
RUN pip install --no-cache-dir uv

# Copy project files (copy lineage first to get lock file)
COPY lineage ./lineage
COPY dbt ./dbt
COPY pyproject.toml ./

# Install dependencies using uv
WORKDIR /app/lineage
# Install dependencies - uv sync will use uv.lock if present
# The --frozen flag ensures exact versions from lock file
RUN uv sync --frozen

# Set Python path for Dagster to find modules
ENV PYTHONPATH=/app/lineage/src
# Add uv's virtual environment to PATH
ENV PATH="/app/lineage/.venv/bin:$PATH"
# Ensure Python uses the venv
ENV VIRTUAL_ENV=/app/lineage/.venv

# Expose gRPC port
EXPOSE 4000
EXPOSE 3000


ENTRYPOINT ["uv", "run", "dagster"]

CMD ["api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "lineage.definitions"]
CMD ["dev", "-h", "0.0.0.0", "-p", "3000"]

