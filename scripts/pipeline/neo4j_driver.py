"""Neo4j driver construction + resilient Cypher execution.

`run_with_retry` is a behavior-preserving extraction of the private
`_neo4j_run_with_retry` previously defined in etl_neo4j.py.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any

from neo4j import GraphDatabase
from neo4j.exceptions import TransientError

from .config import Settings

log = logging.getLogger(__name__)

MAX_RETRIES = 20
_BASE_BACKOFF_SECONDS = 0.1
_MAX_BACKOFF_SECONDS = 10.0


def build_driver(settings: Settings, *, verify: bool = True) -> Any:
    """Construct a Neo4j driver from settings and (optionally) verify connectivity."""
    settings.require_neo4j()
    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    if verify:
        driver.verify_connectivity()
    return driver


def run_with_retry(driver: Any, cypher: str, *, max_retries: int = MAX_RETRIES, **params: Any) -> None:
    """Run a Cypher statement with exponential backoff + jitter on deadlock.

    Behavior matches the previous `_neo4j_run_with_retry` helper. The only
    observable difference is the `max_retries` kwarg for unit-testability.
    """
    for attempt in range(1, max_retries + 1):
        try:
            with driver.session() as session:
                session.run(cypher, **params)
            return
        except TransientError:
            if attempt == max_retries:
                raise
            wait = min(_BASE_BACKOFF_SECONDS * (2 ** attempt) + random.uniform(0, 1.0), _MAX_BACKOFF_SECONDS)
            log.debug("Deadlock (attempt %d/%d), retrying in %.1fs", attempt, max_retries, wait)
            time.sleep(wait)
