FROM python:3.11-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY dagster_project ./dagster_project
COPY pipelines ./pipelines
COPY dbt ./dbt

RUN pip install --no-cache-dir -e .[dev]

EXPOSE 3000
CMD ["dagster", "dev", "-m", "dagster_project", "--host", "0.0.0.0", "--port", "3000"]
