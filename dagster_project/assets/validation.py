"""Validation assets to ensure data quality after transformations."""

from dagster import AssetKey, AssetObservation, MetadataValue, Output, asset


@asset(
    name="dbt_test_suite",
    group_name="quality",
    description="Run dbt test suite to validate the silver mart.",
    required_resource_keys={"dbt"},
    compute_kind="dbt",
)
def dbt_test_asset(context) -> Output[str]:
    # Run `dbt test` across project to validate schema constraints
    test_result = context.resources.dbt.cli(["test"], raise_on_error=False)
    results = test_result.get_results()
    failures = [r for r in results if r.status != "pass"]

    for res in results:
        context.log.info("dbt test %s: %s", res.unique_id, res.status)
        context.observe(
            AssetObservation(
                asset_key=AssetKey(res.unique_id),
                metadata={
                    "status": MetadataValue.text(res.status),
                    "execution_time": MetadataValue.float(res.execution_time),
                },
            )
        )

    if failures:
        raise Exception(f"dbt tests failed: {[f.unique_id for f in failures]}")

    return Output(
        value="dbt tests passed",
        metadata={"tested_nodes": MetadataValue.int(len(results))},
    )
