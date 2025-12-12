"""Run dbt build against a DuckDB profile using synthetic raw tables."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Dict

import pytest

pytest.importorskip(
    "dbt.adapters.duckdb",
    reason="Install dbt-duckdb via project dev extras to run dbt build against DuckDB.",
)
pytest.importorskip(
    "duckdb",
    reason="Install DuckDB dev dependency (pip install .[dev]) to run local/CI DuckDB targets.",
)

import duckdb

from prepare_duckdb_raw import create_empty_raw_tables, raw_columns_by_table


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
        }
    )

    subprocess.run(
        ["dbt", "build", "--project-dir", "dbt", "--profiles-dir", "dbt"],
        check=True,
        env=env,
    )
