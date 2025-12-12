"""Create empty DuckDB raw tables derived from silver model projections.

Used by CI to ensure `dbt build` succeeds against the DuckDB target without
requiring upstream ingestion tables to exist.
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, Iterable, Set

import duckdb


def _extract_columns(sql_path: Path) -> Set[str]:
    columns: Set[str] = set()
    pattern = re.compile(r"as\s+([a-zA-Z0-9_]+)")
    for raw_line in sql_path.read_text().splitlines():
        line = raw_line.strip().rstrip(",")
        if (
            not line
            or line.lower().startswith("select")
            or line.lower().startswith("from")
            or "{{" in line
            or "}}" in line
        ):
            continue
        match = pattern.search(line)
        if match:
            columns.add(match.group(1))
        else:
            cleaned = line.split()[0]
            if cleaned not in {"from", "where"}:
                columns.add(cleaned)
    return columns


def raw_columns_by_table(models_dir: Path) -> Dict[str, Set[str]]:
    mapping: Dict[str, Set[str]] = {}
    for sql_file in models_dir.glob("*.sql"):
        table_name = sql_file.stem
        mapping[table_name] = _extract_columns(sql_file)
    mapping.setdefault("xapi_statements", {"id", "actor", "verb"})
    return mapping


def create_empty_raw_tables(db_path: Path, columns_by_table: Dict[str, Iterable[str]]) -> None:
    conn = duckdb.connect(str(db_path))
    conn.execute("create schema if not exists raw")
    for table, columns in columns_by_table.items():
        column_defs = ", ".join(f"{col} varchar" for col in sorted(columns)) or "dummy varchar"
        conn.execute(f"create or replace table raw.{table} ({column_defs})")
    conn.close()


def main() -> None:
    models_dir = Path(__file__).resolve().parent / "dbt" / "models" / "silver"
    db_path = Path(os.environ.get("DUCKDB_DATABASE", "/tmp/ci_duckdb.db"))
    columns_by_table = raw_columns_by_table(models_dir)
    create_empty_raw_tables(db_path, columns_by_table)
    print(f"Prepared {len(columns_by_table)} raw tables in {db_path}")


if __name__ == "__main__":
    main()
