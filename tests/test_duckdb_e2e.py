"""End-to-end tests that validate ingestion against DuckDB without BigQuery access."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import duckdb
import dlt

from pipelines.mongo import load_mongo_raw
from pipelines.xapi import load_xapi_raw


@dlt.source(name="mock_mongo")
def mock_mongo_source():
    @dlt.resource(name="dim_organization")
    def dim_organization():
        yield {"organization_id": "org-1", "name": "Demo Org", "timezone": "UTC"}

    @dlt.resource(name="dim_course")
    def dim_course():
        yield {
            "course_id": "course-1",
            "organization_id": "org-1",
            "code": "ALG-1",
            "name": "Algebra I",
        }

    @dlt.resource(name="fact_learning_event")
    def fact_learning_event():
        yield {
            "event_id": "evt-1",
            "time_id": 1,
            "event_timestamp": "2024-01-01T00:00:00Z",
            "organization_id": "org-1",
            "course_id": "course-1",
        }

    return dim_organization, dim_course, fact_learning_event


def _duckdb_pipeline_kwargs(temp_dir: Path, db_name: str) -> dict:
    return {
        "pipelines_dir": str(temp_dir / "pipelines"),
        "destination_kwargs": {"database": str(temp_dir / db_name)},
    }


def test_mongo_roundtrip_in_duckdb(tmp_path: Path) -> None:
    """Ensure Mongo ingestion can target DuckDB for local/CI execution."""

    load_mongo_raw(
        collections=["dim_organization", "dim_course", "fact_learning_event"],
        destination="duckdb",
        dataset_name="raw",
        pipeline_kwargs=_duckdb_pipeline_kwargs(tmp_path, "mongo.db"),
        source=mock_mongo_source(),
    )

    conn = duckdb.connect(str(tmp_path / "mongo.db"))
    assert conn.execute("select count(*) from raw.dim_organization").fetchone()[0] == 1
    assert conn.execute("select count(*) from raw.fact_learning_event").fetchone()[0] == 1


def test_xapi_roundtrip_in_duckdb(tmp_path: Path) -> None:
    """Validate xAPI ingestion using a provided fetcher without hitting the network."""

    sample_statements: Iterable[dict] = [
        {"id": "stmt-1", "actor": {"mbox": "mailto:user@example.com"}, "verb": {"id": "completed"}},
        {"id": "stmt-2", "actor": {"mbox": "mailto:user@example.com"}, "verb": {"id": "initialized"}},
    ]

    load_xapi_raw(
        destination="duckdb",
        dataset_name="raw",
        pipeline_kwargs=_duckdb_pipeline_kwargs(tmp_path, "xapi.db"),
        fetcher=sample_statements,
    )

    conn = duckdb.connect(str(tmp_path / "xapi.db"))
    assert conn.execute("select count(*) from raw.xapi_statements").fetchone()[0] == 2
