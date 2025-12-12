"""Run dbt build against a DuckDB profile using synthetic raw tables."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path
from typing import Dict, Iterable, Set

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


def _extract_columns(sql_path: Path) -> Set[str]:
    columns: Set[str] = set()
    pattern = re.compile(r"as\s+([a-zA-Z0-9_]+)")
    for raw_line in sql_path.read_text().splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.lower().startswith("select") or line.lower().startswith("from"):
            continue
        match = pattern.search(line)
        if match:
            columns.add(match.group(1))
        else:
            # handle bare column references like metadata
            cleaned = line.split()[0]
            if cleaned not in {"from", "where"}:
                columns.add(cleaned)
    return columns


def _raw_columns_by_table(models_dir: Path) -> Dict[str, Set[str]]:
    mapping: Dict[str, Set[str]] = {}
    for sql_file in models_dir.glob("*.sql"):
        table_name = sql_file.stem
        mapping[table_name] = _extract_columns(sql_file)
    # Ensure xAPI raw table is present even though it lacks a silver model today
    mapping.setdefault("xapi_statements", {"id", "actor", "verb"})
    return mapping


def _create_empty_raw_tables(db_path: Path, columns_by_table: Dict[str, Iterable[str]]) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute("create schema if not exists raw")
    for table, columns in columns_by_table.items():
        column_defs = ", ".join(f"{col} varchar" for col in sorted(columns)) or "dummy varchar"
        conn.execute(f"create or replace table raw.{table} ({column_defs})")
    conn.close()


@pytest.mark.integration
def test_dbt_build_against_duckdb(tmp_path: Path) -> None:
    """Materialize silver models and schema tests against DuckDB using synthetic raw tables."""

    db_path = tmp_path / "ci_duckdb.db"
    columns_by_table = _raw_columns_by_table(Path("dbt/models/silver"))
    _create_empty_raw_tables(db_path, columns_by_table)

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
