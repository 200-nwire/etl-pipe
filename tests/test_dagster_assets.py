"""Dagster asset tests using in-memory materialization.

Exercises ingestion assets with stubbed dlt loaders and the dbt test suite
asset with a fake dbt resource so the Dagster orchestration layer is covered in
CI without hitting external systems.
"""

from __future__ import annotations

from typing import Iterable, List

import pytest

dagster = pytest.importorskip("dagster")
from dagster import ResourceDefinition, materialize_to_memory, with_resources

from dagster_project.assets.ingestion import mongo_raw_asset, xapi_raw_asset
from dagster_project.assets.validation import dbt_test_asset


def test_ingestion_assets_materialize(monkeypatch):
    """Ingestion assets run end-to-end with in-memory outputs."""

    mongo_tables: List[str] = ["raw.users", "raw.orders"]
    xapi_table = "raw.xapi_statements"

    monkeypatch.setattr(
        "dagster_project.assets.ingestion.load_mongo_raw", lambda: mongo_tables
    )
    monkeypatch.setattr("dagster_project.assets.ingestion.load_xapi_raw", lambda: xapi_table)

    asset_defs = with_resources(
        [mongo_raw_asset, xapi_raw_asset],
        {"secrets": ResourceDefinition.none_resource()},
    )

    result = materialize_to_memory(asset_defs)

    assert result.success
    assert result.output_for_node("mongo_raw_ingestion")
    assert result.output_for_node("xapi_raw_ingestion") == xapi_table

    mongo_materialization = result.asset_materializations_for_node("mongo_raw_ingestion")[0]
    assert mongo_materialization.metadata["raw_tables"].data == mongo_tables


class _FakeDbtResult:
    def __init__(self, unique_id: str, status: str = "pass", execution_time: float = 0.1):
        self.unique_id = unique_id
        self.status = status
        self.execution_time = execution_time


class _FakeDbtInvocation:
    def __init__(self, results: Iterable[_FakeDbtResult]):
        self._results = list(results)

    def get_results(self):
        return self._results


class _FakeDbtResource:
    def __init__(self, results: Iterable[_FakeDbtResult]):
        self._invocation = _FakeDbtInvocation(results)

    def cli(self, args, raise_on_error=True):  # noqa: ARG002
        return self._invocation


def test_dbt_test_asset_passes_with_fake_resource():
    """dbt_test_asset surfaces pass/fail information from the dbt resource."""

    results = [_FakeDbtResult(unique_id="model.etl_silver.users"), _FakeDbtResult("test.pk")] 

    asset_defs = with_resources(
        [dbt_test_asset],
        {"dbt": _FakeDbtResource(results)},
    )

    materialize_result = materialize_to_memory(asset_defs)

    assert materialize_result.success
    assert materialize_result.output_for_node("dbt_test_suite") == "dbt tests passed"
