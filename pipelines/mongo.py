"""dlt pipeline for MongoDB raw ingestion into BigQuery."""

from typing import List, Optional

import dlt
from dlt.sources.mongo import mongo

DEFAULT_MONGO_COLLECTIONS = [
    "users",
    "schools",
    "grades",
    "disciplines",
    "skills",
    "stages",
    "personas",
    "courses",
    "modules",
    "lessons",
    "pages",
    "blocks",
    "exercises",
    "exercise_submissions",
    "enrollments",
    "projects",
    "project_enrollments",
    "surveys",
    "survey_enrollments",
    "survey_submissions",
    "questionnaires",
    "assessment_profiles",
    "events",
    "notifications",
    "login",
    "rules",
    "config",
    "versions",
    "meeting",
]


def load_mongo_raw(
    collections: Optional[List[str]] = None,
    *,
    destination: str = "bigquery",
    dataset_name: str = "raw",
    pipeline_kwargs: Optional[dict] = None,
    source: Optional[dlt.sources.DltSource] = None,
) -> List[str]:
    """
    Load MongoDB collections extracted by Airbyte into BigQuery raw dataset using dlt.

    Connection details are sourced from environment variables consumed by dlt's MongoDB
    source (e.g. MONGO_USER, MONGO_PASSWORD, MONGO_DATABASE, MONGO_HOST, MONGO_PORT).
    """

    selected_collections = collections or DEFAULT_MONGO_COLLECTIONS
    pipeline = dlt.pipeline(
        pipeline_name="mongo_raw",
        destination=destination,
        dataset_name=dataset_name,
        full_refresh=False,
        **(pipeline_kwargs or {}),
    )

    active_source = source or mongo()
    # When using the Mongo connector we keep existing behavior of applying empty filters.
    if source is None:
        for coll in selected_collections:
            active_source[coll].add_filter({})

    pipeline.run(active_source.with_resources(*selected_collections))
    return list(selected_collections)
