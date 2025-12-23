"""End-to-end tests that validate ingestion against DuckDB without BigQuery access."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pytest

pytest.importorskip(
    "dlt",
    reason="Install project dev extras (pip install .[dev]) to exercise DuckDB ingestion.",
)
pytest.importorskip(
    "dlt.sources.mongo",
    reason="Install dlt Mongo extra (pip install 'dlt[mongo]') for end-to-end ingestion tests.",
)
pytest.importorskip(
    "duckdb",
    reason="Install DuckDB dev dependency (pip install .[dev]) to run local/CI DuckDB targets.",
)

import dlt
import duckdb

from lineage.sources.mongo import load_mongo_raw
from lineage.sources.xapi import load_xapi_raw


@dlt.source(name="mock_mongo")
def mock_mongo_source():
    @dlt.resource(name="users")
    def users():
        yield {
            "_id": "user-1",
            "first_name": "Ada",
            "last_name": "Lovelace",
            "role": "student",
            "grade": "10",
            "coins": 42,
        }

    @dlt.resource(name="courses")
    def courses():
        yield {
            "_id": "course-1",
            "title": "Algorithms",
            "grade": "10",
            "skills": [],
        }

    @dlt.resource(name="enrollments")
    def enrollments():
        yield {
            "_id": "enroll-1",
            "student": "user-1",
            "course": "course-1",
            "status": "started",
            "total_lessons": 1,
        }

    return users, courses, enrollments


def _duckdb_pipeline_kwargs(temp_dir: Path, db_name: str) -> dict:
    """Return kwargs for DuckDB destination."""
    import os
    os.environ["DUCKDB_DATABASE"] = str(temp_dir / db_name)
    return {}


def test_mongo_roundtrip_in_duckdb(tmp_path: Path) -> None:
    """Ensure Mongo ingestion can target DuckDB for local/CI execution."""

    load_mongo_raw(
        collections=["users", "courses", "enrollments"],
        destination="duckdb",
        dataset_name="raw",
        pipeline_kwargs=_duckdb_pipeline_kwargs(tmp_path, "mongo.db"),
        source=mock_mongo_source(),
    )

    conn = duckdb.connect(str(tmp_path / "mongo.db"))
    assert conn.execute("select count(*) from raw.users").fetchone()[0] == 1
    assert conn.execute("select count(*) from raw.enrollments").fetchone()[0] == 1


def test_xapi_roundtrip_in_duckdb(tmp_path: Path) -> None:
    """Validate xAPI ingestion using a provided fetcher without hitting the network."""

    sample_statements: Iterable[dict] = [
        {
            "id": "stmt-1",
            "actor": {"mbox": "mailto:user@example.com"},
            "verb": {"id": "completed"},
        },
        {
            "id": "stmt-2",
            "actor": {"mbox": "mailto:user@example.com"},
            "verb": {"id": "initialized"},
        },
    ]

    load_xapi_raw(
        destination="duckdb",
        dataset_name="raw",
        pipeline_kwargs=_duckdb_pipeline_kwargs(tmp_path, "xapi.db"),
        fetcher=sample_statements,
    )

    conn = duckdb.connect(str(tmp_path / "xapi.db"))
    assert conn.execute("select count(*) from raw.xapi_statements").fetchone()[0] == 2
