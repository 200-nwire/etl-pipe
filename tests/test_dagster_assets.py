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
    "dlt",
    reason="Install dlt to run Dagster asset tests.",
)


def test_ingestion_assets_materialize(monkeypatch):
    """Ingestion assets run end-to-end with in-memory outputs."""

    from dagster import AssetSelection, materialize_to_memory, with_resources
    from lineage.definitions import defs

    # Mock the load functions to avoid actual BigQuery/dlt calls
    monkeypatch.setattr(
        "lineage.sources.lms.load_mongo_raw", lambda **kwargs: ["lms_users", "lms_courses"]
    )
    monkeypatch.setattr("lineage.sources.lrs.load_xapi_raw", lambda **kwargs: ["lrs_statements"])

    # Get raw assets from definitions (created by dlt component)
    all_defs = defs
    
    # Handle both single-asset and multi-asset definitions
    def get_asset_keys(asset_def):
        """Get asset key(s) from an asset definition."""
        if hasattr(asset_def, 'keys') and asset_def.keys:
            return list(asset_def.keys) if isinstance(asset_def.keys, (set, frozenset)) else list(asset_def.keys)
        elif hasattr(asset_def, 'key'):
            return [asset_def.key]
        else:
            return []
    
    # Find raw assets (assets with key starting with "raw")
    raw_assets = []
    for asset in all_defs.assets:
        keys = get_asset_keys(asset)
        raw_keys = [k for k in keys if len(k.path) > 0 and k.path[0] == "raw"]
        if raw_keys:
            raw_assets.append(asset)
    
    if not raw_assets:
        pytest.skip("No raw assets found - dlt component may not be loaded")
    
    # Select first raw asset for testing
    test_assets = raw_assets[:1]
    
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
