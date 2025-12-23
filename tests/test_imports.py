import importlib

import pytest

pytest.importorskip(
    "dlt",
    reason="Install project dev extras (pip install .[dev]) to load the dlt pipelines.",
)


def test_lineage_imports():
    """Test that lineage project modules can be imported."""
    assert importlib.import_module("lineage")
    assert importlib.import_module("lineage.sources.lms")
    assert importlib.import_module("lineage.sources.lrs")
    # Also test that the __init__ exports work
    from lineage.sources import load_mongo_raw, load_xapi_raw
    assert load_mongo_raw is not None
    assert load_xapi_raw is not None
