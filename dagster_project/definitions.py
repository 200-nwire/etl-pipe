"""Dagster definitions for orchestrating ingestion and dbt transformations."""

from dagster import Definitions, EnvVar, ResourceDefinition
from dagster_dbt import DbtCliResource, load_assets_from_dbt_project

from dagster_project.assets.ingestion import mongo_raw_asset, xapi_raw_asset
from dagster_project.assets.validation import dbt_test_asset

DBT_PROJECT_PATH = "dbt"


# dbt assets are discovered dynamically from the project

dbt_assets = load_assets_from_dbt_project(
    project_dir=DBT_PROJECT_PATH,
    profiles_dir=DBT_PROJECT_PATH,
)


defs = Definitions(
    assets=[mongo_raw_asset, xapi_raw_asset, dbt_assets, dbt_test_asset],
    resources={
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_PATH,
            profiles_dir=DBT_PROJECT_PATH,
            target=EnvVar("DBT_TARGET", default="dev"),
        ),
        # Placeholder resource for external secrets managers
        "secrets": ResourceDefinition.none_resource(),
    },
)
