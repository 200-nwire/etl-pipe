"""dlt pipeline for MongoDB raw ingestion into BigQuery."""

from typing import List

import dlt
from dlt.sources.mongo import mongo


DEFAULT_MONGO_COLLECTIONS = [
    "dim_organization",
    "dim_learner",
    "dim_teacher",
    "dim_course",
    "dim_section",
    "dim_content_item",
    "dim_assessment",
    "dim_skill",
    "fact_learning_event",
    "fact_assessment_attempt",
]


def load_mongo_raw(collections: List[str] | None = None) -> List[str]:
    """
    Load MongoDB collections extracted by Airbyte into BigQuery raw dataset using dlt.

    Connection details are sourced from environment variables consumed by dlt's MongoDB
    source (e.g. MONGO_USER, MONGO_PASSWORD, MONGO_DATABASE, MONGO_HOST, MONGO_PORT).
    """

    selected_collections = collections or DEFAULT_MONGO_COLLECTIONS
    pipeline = dlt.pipeline(
        pipeline_name="mongo_raw",
        destination="bigquery",
        dataset_name="raw",
        full_refresh=False,
    )

    source = mongo()
    for coll in selected_collections:
        source[coll].add_filter({})

    load_info = pipeline.run(source.with_resources(*selected_collections))
    return [table.name for table in load_info.loads_ids.values()]
