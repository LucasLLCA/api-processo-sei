"""JsonFileReader — replay an emit directory produced by JsonFileWriter.

Reads the Phase B activity data out of ``templates/load_atividades.ndjson``
(that's where `execute_template` stashed the rows that went to the composite
LOAD_ATIVIDADES Cypher). Each line has shape::

    {"name": "load_atividades",
     "cypher": "...",
     "params": {"rows": [ {row1}, {row2}, ... ]},
     "phase": "B"}

Each row already carries everything Phase C and Phase D need:
`protocolo_formatado, source_id, data_hora, tipo_acao, ref_id, unidade,
source_unidade`. The reader groups rows by ``protocolo_formatado``, sorts
each group by ``(data_hora, source_id)``, and yields batches of
`ProcessoTimeline` objects.

Memory model
------------
This is a one-shot full load. On the full SEAD dataset that's potentially
millions of activities in memory. For step 10 scope that's acceptable; a
streaming materialized-view mode is a future optimization documented in
the plan.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Iterator

from .base import GraphReader, ProcessoTimeline, ReaderError

log = logging.getLogger(__name__)

_ACTIVITY_FIELDS = (
    "source_id",
    "data_hora",
    "tipo_acao",
    "ref_id",
    "unidade",
    "source_unidade",
)


class JsonFileReader(GraphReader):
    def __init__(self, input_dir: Path | str) -> None:
        self._input_dir = Path(input_dir)
        if not self._input_dir.is_dir():
            raise ReaderError(f"JsonFileReader: {self._input_dir} is not a directory")
        self._timelines: list[ProcessoTimeline] | None = None

    # -- Lifecycle ---------------------------------------------------------

    def close(self) -> None:
        self._timelines = None

    # -- Queries -----------------------------------------------------------

    def count_processos(self) -> int:
        return len(self._load())

    def iter_processo_batches(
        self, *, batch_size: int = 500,
    ) -> Iterator[list[ProcessoTimeline]]:
        timelines = self._load()
        if not timelines:
            return
        for i in range(0, len(timelines), batch_size):
            yield timelines[i:i + batch_size]

    # -- Internals ---------------------------------------------------------

    def _load(self) -> list[ProcessoTimeline]:
        if self._timelines is not None:
            return self._timelines

        template_path = self._input_dir / "templates" / "load_atividades.ndjson"
        if not template_path.is_file():
            raise ReaderError(
                f"{template_path} not found. Phase B template output is required "
                f"for JsonFileReader. Run `python -m pipeline.etl --emit-json {self._input_dir}` first."
            )

        log.info("JsonFileReader: loading activities from %s", template_path)
        by_processo: dict[str, list[dict]] = defaultdict(list)
        total_rows = 0

        with template_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                rows = record.get("params", {}).get("rows", []) or []
                for row in rows:
                    pf = row.get("protocolo_formatado")
                    if not pf:
                        continue
                    by_processo[pf].append(
                        {k: row.get(k) for k in _ACTIVITY_FIELDS}
                    )
                    total_rows += 1

        for acts in by_processo.values():
            acts.sort(key=lambda a: (a["data_hora"] or "", a["source_id"] or ""))

        timelines = [
            ProcessoTimeline(protocolo_formatado=pf, activities=by_processo[pf])
            for pf in sorted(by_processo.keys())
        ]
        log.info(
            "JsonFileReader: loaded %d processos, %d activities",
            len(timelines), total_rows,
        )
        self._timelines = timelines
        return timelines
