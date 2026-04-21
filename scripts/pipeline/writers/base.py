"""Abstract writer interface.

Every ETL write path goes through a `GraphWriter`. There are three entry
points so that as much output as possible is expressed as structured
node/edge operations (intercep-friendly for `JsonFileWriter`) with an
`execute_template` escape hatch for composite Cypher statements that fuse
multiple MERGE/MATCH clauses into one transaction.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping


class WriterError(RuntimeError):
    """Raised when a writer cannot fulfill a request (bad shape, I/O failure)."""


class GraphWriter(ABC):
    """Abstract graph-writing seam used by all ETL scripts.

    `phase` is an opaque label (e.g. "A", "B", "C", "schema") used for
    checkpointing and diagnostics. Concrete writers may persist it.

    Row shape conventions
    ---------------------
    `write_nodes` takes an iterable of flat dicts. `key_fields` are the
    MERGE key (required to be present in every row). Every other field in
    the row is written as a node property unless `props` is given, in which
    case only listed non-key fields are written.

    `write_edges` takes an iterable of flat dicts. `from_key` / `to_key`
    map **row field names** to **node property names** on the endpoint
    label, so the same row can disambiguate two endpoints that share a
    property name (e.g. `SUBUNIDADE_DE` between two `Unidade` nodes keyed
    on `sigla` via row fields ``child`` / ``parent``). Optional `props`
    lists row fields to SET on the relationship.
    """

    # -- Lifecycle ---------------------------------------------------------

    def open_phase(self, phase: str) -> None:  # pragma: no cover - trivial
        """Signal the start of a logical phase. Default is a no-op."""

    def close_phase(self, phase: str) -> None:  # pragma: no cover - trivial
        """Signal the end of a logical phase. Default is a no-op."""

    @abstractmethod
    def close(self) -> None:
        """Release any underlying resources."""

    def __enter__(self) -> "GraphWriter":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- Structured writes -------------------------------------------------

    @abstractmethod
    def write_nodes(
        self,
        label: str,
        key_fields: list[str],
        rows: Iterable[Mapping[str, Any]],
        *,
        phase: str,
        props: list[str] | None = None,
    ) -> int:
        """MERGE nodes with `label` keyed on `key_fields`, setting other fields."""

    @abstractmethod
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
        """MERGE `(:from_label)-[:rel_type]->(:to_label)` edges."""

    # -- Composite / opaque escape hatch -----------------------------------

    @abstractmethod
    def execute_template(
        self,
        name: str,
        cypher: str,
        params: Mapping[str, Any],
        *,
        phase: str,
    ) -> int:
        """Run a raw Cypher template with the given params.

        Used for composite MERGE/MATCH statements that cannot be expressed
        via `write_nodes`/`write_edges`.
        """
