"""Dagster asset tests using in-memory materialization.

Exercises ingestion assets with stubbed dlt loaders and the dbt test suite
asset with a fake dbt resource so the Dagster orchestration layer is covered in
CI without hitting external systems.
"""

from __future__ import annotations

from typing import Iterable, List

import pytest

pytest.importorskip("dagster")
pytest.importorskip(
    "dlt.sources.mongo",
    reason="Install dlt mongo extra to run Dagster asset tests.",
)
pytest.importorskip(
    "dlt.sources.rest_api",
    reason="Install dlt rest_api extra to run Dagster asset tests.",
)


def test_ingestion_assets_materialize(monkeypatch):
    """Ingestion assets run end-to-end with in-memory outputs."""

    from dagster import ResourceDefinition, materialize_to_memory, with_resources

    from lineage.defs.raw_tables import mongo_raw_assets, xapi_raw_table_asset

    mongo_tables: List[str] = ["raw.lms_users", "raw.lms_courses"]
    xapi_table = "raw.xapi_statements"

    # Mock the load functions
    monkeypatch.setattr(
        "lineage.sources.mongo.load_mongo_raw", lambda **kwargs: "raw"
    )
    monkeypatch.setattr("lineage.sources.xapi.load_xapi_raw", lambda **kwargs: "raw")

    # Use first mongo asset and xapi asset for testing
    test_assets = [mongo_raw_assets[0], xapi_raw_table_asset]
    
    asset_defs = with_resources(
        test_assets,
        {},
    )

    result = materialize_to_memory(asset_defs)

    assert result.success
    # Check that assets materialized successfully
    assert len(result.asset_materializations) > 0


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

    from dagster import materialize_to_memory, with_resources

    # Note: validation asset was removed, so this test is skipped
    pytest.skip("Validation asset removed from lineage project")

    results = [_FakeDbtResult(unique_id="model.etl_silver.users"), _FakeDbtResult("test.pk")] 

    asset_defs = with_resources(
        [dbt_test_asset],
        {"dbt": _FakeDbtResource(results)},
    )

    materialize_result = materialize_to_memory(asset_defs)

    assert materialize_result.success
    assert materialize_result.output_for_node("dbt_test_suite") == "dbt tests passed"
