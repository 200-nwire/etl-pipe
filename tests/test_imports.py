import importlib

import pytest

pytest.importorskip(
    "dlt",
    reason="Install project dev extras (pip install .[dev]) to load the dlt pipelines.",
)
pytest.importorskip(
    "dlt.sources.mongo",
    reason="Install dlt Mongo extra (pip install 'dlt[mongo]') to import Mongo pipeline helpers.",
)


def test_lineage_imports():
    """Test that lineage project modules can be imported."""
    assert importlib.import_module("lineage")
    assert importlib.import_module("lineage.sources.mongo")
    assert importlib.import_module("lineage.sources.xapi")
