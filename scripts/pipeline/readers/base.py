"""Abstract graph reader.

`GraphReader` is the seam Phase C (timeline DAG) and Phase D (permanência)
go through to fetch each processo's activity timeline. Readers yield
*batches* of processos so callers can preserve their per-batch write flush
pattern.

Activity shape
--------------
Each processo's `activities` list is a plain `list[dict[str, Any]]`. Each
dict must carry (unified across Phase C + Phase D needs):

- `source_id`        (str)
- `data_hora`        (str, ISO-8601 timestamp)
- `tipo_acao`        (str | None)
- `ref_id`           (str | None)
- `unidade`          (str | None)
- `source_unidade`   (str | None)   # only populated on PROCESSO-REMETIDO events

Activities within a processo are ordered by (data_hora, source_id) ascending.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Iterator


class ReaderError(RuntimeError):
    """Raised when a reader cannot fulfill a request."""


@dataclass
class ProcessoTimeline:
    protocolo_formatado: str
    activities: list[dict[str, Any]]


class GraphReader(ABC):
    """Abstract seam for reading processo activity timelines.

    Concrete readers must implement `count_processos` and
    `iter_processo_batches`; everything else has sensible defaults.
    """

    # -- Lifecycle ---------------------------------------------------------

    def __enter__(self) -> "GraphReader":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    @abstractmethod
    def close(self) -> None:
        """Release any underlying resources."""

    # -- Queries -----------------------------------------------------------

    @abstractmethod
    def count_processos(self) -> int:
        """Total number of processos available."""

    @abstractmethod
    def iter_processo_batches(
        self, *, batch_size: int = 500,
    ) -> Iterator[list[ProcessoTimeline]]:
        """Yield processos in groups so callers can flush writes per batch.

        Each yielded list contains up to `batch_size` processos, each with
        its activities pre-sorted by (data_hora, source_id).
        """
