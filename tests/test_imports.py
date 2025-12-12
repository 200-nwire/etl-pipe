import importlib

import pytest

pytest.importorskip("dlt")
pytest.importorskip("dlt.sources.mongo")


def test_dagster_project_imports():
    assert importlib.import_module("dagster_project")
    assert importlib.import_module("pipelines.mongo")
    assert importlib.import_module("pipelines.xapi")
