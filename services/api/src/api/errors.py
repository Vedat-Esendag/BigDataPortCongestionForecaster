"""What the API does when a store is down, and when a stored record is broken."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

from prometheus_client import Counter
from pydantic import BaseModel, ValidationError

log = logging.getLogger(__name__)

INVALID_DOCUMENTS = Counter(
    "api_invalid_documents_total",
    "Stored records the API skipped because they don't match the response model",
    ["store"],
)


class StoreUnavailable(Exception):
    """Redis or Cassandra can't be reached. The API turns this into HTTP 503.

    503 rather than 500 tells the dashboard (and Kubernetes) that this is temporary and that
    nothing is wrong with the request itself.
    """

    def __init__(self, store: str, reason: str) -> None:
        super().__init__(f"{store} is unavailable: {reason}")
        self.store = store
        self.reason = reason


def parse_documents[Model: BaseModel](
    model: type[Model], documents: Iterable[dict[str, Any]], store: str
) -> list[Model]:
    """Turn stored records into response models, skipping the ones that don't fit.

    One broken record (a missing field, a wrong type) should not take down the whole ship list.
    It is skipped, logged and counted in api_invalid_documents_total. The project rule is that
    bad records are counted, never silently dropped.
    """
    parsed: list[Model] = []
    for document in documents:
        try:
            parsed.append(model.model_validate(document))
        except ValidationError as error:
            INVALID_DOCUMENTS.labels(store=store).inc()
            log.warning(
                "Skipping a %s record that doesn't fit %s: %s", store, model.__name__, error
            )
    return parsed
