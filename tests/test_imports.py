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


def test_dagster_project_imports():
    assert importlib.import_module("dagster_project")
    assert importlib.import_module("pipelines.mongo")
    assert importlib.import_module("pipelines.xapi")
