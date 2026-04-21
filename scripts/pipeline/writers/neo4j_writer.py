"""Direct Neo4j implementation of `GraphWriter`.

Generates UNWIND-based MERGE Cypher for `write_nodes` and `write_edges`, and
forwards `execute_template` calls straight to the driver. Every statement is
executed via `pipeline.neo4j_driver.run_with_retry` so deadlock retries are
preserved.
"""

from __future__ import annotations

import logging
from typing import Any, Iterable, Mapping

from ..neo4j_driver import run_with_retry
from .base import GraphWriter, WriterError

log = logging.getLogger(__name__)


def _materialize(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Force-eager so we can inspect length + iterate multiple times."""
    return [dict(r) for r in rows]


class DirectNeo4jWriter(GraphWriter):
    def __init__(self, driver: Any, *, batch_size: int = 1000) -> None:
        self._driver = driver
        self._batch_size = batch_size

    # -- Lifecycle ---------------------------------------------------------

    def open_phase(self, phase: str) -> None:
        log.debug("Neo4jWriter: entering phase %s", phase)

    def close_phase(self, phase: str) -> None:
        log.debug("Neo4jWriter: leaving phase %s", phase)

    def close(self) -> None:
        # Caller owns the driver lifetime; do not close it here.
        pass

    # -- Cypher generation -------------------------------------------------

    @staticmethod
    def _build_node_cypher(label: str, key_fields: list[str], prop_fields: list[str]) -> str:
        key_block = ", ".join(f"{k}: r.{k}" for k in key_fields)
        lines = [
            "UNWIND $rows AS r",
            f"MERGE (n:{label} {{{key_block}}})",
        ]
        if prop_fields:
            set_block = ", ".join(f"n.{p} = r.{p}" for p in prop_fields)
            lines.append(f"SET {set_block}")
        return "\n".join(lines)

    @staticmethod
    def _build_edge_cypher(
        rel_type: str,
        from_label: str,
        from_key: Mapping[str, str],
        to_label: str,
        to_key: Mapping[str, str],
        prop_fields: list[str],
    ) -> str:
        from_block = ", ".join(f"{node_prop}: r.{row_field}" for row_field, node_prop in from_key.items())
        to_block = ", ".join(f"{node_prop}: r.{row_field}" for row_field, node_prop in to_key.items())
        lines = [
            "UNWIND $rows AS r",
            f"MATCH (a:{from_label} {{{from_block}}})",
            f"MATCH (b:{to_label} {{{to_block}}})",
            f"MERGE (a)-[rel:{rel_type}]->(b)",
        ]
        if prop_fields:
            set_block = ", ".join(f"rel.{p} = r.{p}" for p in prop_fields)
            lines.append(f"SET {set_block}")
        return "\n".join(lines)

    # -- Structured writes -------------------------------------------------

    def write_nodes(
        self,
        label: str,
        key_fields: list[str],
        rows: Iterable[Mapping[str, Any]],
        *,
        phase: str,
        props: list[str] | None = None,
    ) -> int:
        data = _materialize(rows)
        if not data:
            return 0
        if not key_fields:
            raise WriterError(f"write_nodes({label}) requires at least one key field")
        if props is None:
            key_set = set(key_fields)
            props = [k for k in data[0].keys() if k not in key_set]
        cypher = self._build_node_cypher(label, key_fields, props)
        self._run_in_batches(cypher, data)
        return len(data)

    def write_edges(
        self,
        rel_type: str,
        from_label: str,
        from_key: Mapping[str, str],
        to_label: str,
        to_key: Mapping[str, str],
        rows: Iterable[Mapping[str, Any]],
        *,
        phase: str,
        props: list[str] | None = None,
    ) -> int:
        data = _materialize(rows)
        if not data:
            return 0
        if not from_key or not to_key:
            raise WriterError(f"write_edges({rel_type}) requires non-empty from_key and to_key")
        cypher = self._build_edge_cypher(rel_type, from_label, from_key, to_label, to_key, props or [])
        self._run_in_batches(cypher, data)
        return len(data)

    # -- Composite escape hatch --------------------------------------------

    def execute_template(
        self,
        name: str,
        cypher: str,
        params: Mapping[str, Any],
        *,
        phase: str,
    ) -> int:
        run_with_retry(self._driver, cypher, **dict(params))
        return 1

    # -- Internals ---------------------------------------------------------

    def _run_in_batches(self, cypher: str, rows: list[dict[str, Any]]) -> None:
        step = self._batch_size or len(rows) or 1
        for i in range(0, len(rows), step):
            batch = rows[i:i + step]
            run_with_retry(self._driver, cypher, rows=batch)
