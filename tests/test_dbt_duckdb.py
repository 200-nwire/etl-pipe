"""Run dbt build against a DuckDB profile using synthetic raw tables."""

from __future__ import annotations

import importlib
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

pyproject_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(pyproject_root))

pytest.importorskip(
    "dbt.adapters.duckdb",
    reason="Install dbt-duckdb via project dev extras to run dbt build against DuckDB.",
)
pytest.importorskip(
    "duckdb",
    reason="Install DuckDB dev dependency (pip install .[dev]) to run local/CI DuckDB targets.",
)

if importlib.util.find_spec("prepare_duckdb_raw") is None:
    pytest.skip("prepare_duckdb_raw helper is not importable in this environment.")

prepare_duckdb_raw = importlib.import_module("prepare_duckdb_raw")
create_empty_raw_tables = prepare_duckdb_raw.create_empty_raw_tables
raw_columns_by_table = prepare_duckdb_raw.raw_columns_by_table


@pytest.mark.integration
def test_dbt_build_against_duckdb(tmp_path: Path) -> None:
    """Materialize silver models and schema tests against DuckDB using synthetic raw tables."""

    db_path = tmp_path / "ci_duckdb.db"
    columns_by_table = raw_columns_by_table(Path("dbt/models/silver"))
    create_empty_raw_tables(db_path, columns_by_table)

    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(Path("dbt").resolve()),
            "DBT_TARGET": "duckdb",
            "DUCKDB_DATABASE": str(db_path),
            "DBT_DUCKDB_SCHEMA": "raw_mongo",
            "DBT_RAW_SCHEMA": "raw_mongo",
        }
    )

    subprocess.run(
        ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        check=True,
        env=env,
    )
