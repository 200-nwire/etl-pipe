"""dlt pipeline for ingesting xAPI LRS statements into BigQuery."""

from __future__ import annotations

import os
from typing import Dict, Iterable

import dlt
import requests


XAPI_PAGE_SIZE = 500


def _fetch_xapi_statements() -> Iterable[Dict]:
    """Simple generator fetching paginated xAPI statements using the LRS REST API."""

    endpoint = os.environ.get("XAPI_LRS_ENDPOINT")
    auth_token = os.environ.get("XAPI_AUTH_TOKEN")
    if not endpoint:
        raise RuntimeError("XAPI_LRS_ENDPOINT is required for xAPI ingestion")

    headers = {"Authorization": f"Bearer {auth_token}"} if auth_token else {}
    params: Dict[str, str | int] = {"limit": XAPI_PAGE_SIZE}
    more = True
    while more:
        response = requests.get(endpoint, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()
        statements = payload.get("statements", [])
        for stmt in statements:
            yield stmt

        more_url = payload.get("more")
        if more_url:
            endpoint = more_url if more_url.startswith("http") else os.path.join(endpoint, more_url)
        else:
            more = False


def load_xapi_raw() -> str:
    """Load xAPI statements into the raw dataset using dlt."""

    pipeline = dlt.pipeline(
        pipeline_name="xapi_raw",
        destination="bigquery",
        dataset_name="raw",
        full_refresh=False,
    )

    @dlt.resource(name="xapi_statements")
    def xapi_resource():
        yield from _fetch_xapi_statements()

    load_info = pipeline.run(xapi_resource())
    return load_info.default_schema_name
