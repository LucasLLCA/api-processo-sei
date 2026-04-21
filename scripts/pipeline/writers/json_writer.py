"""NDJSON emit writer.

Writes a flat semantic nodes+edges representation of the graph under an
output directory. Intended to be inspectable, diffable, and replayable via
`pipeline.replay` into a real Neo4j instance.

Layout::

    <output-dir>/
      manifest.json                    # schema version, phase checkpoints, counts
      nodes/<snake_case_label>.ndjson
      edges/<snake_case_rel>.ndjson
      templates/<template_name>.ndjson # composite MERGE/MATCH escape hatch

Line shapes::

    # nodes/*.ndjson
    {"label": "Unidade",
     "key":   {"sigla": "SEAD-PI/GAB"},
     "props": {"id_unidade": 42, "descricao": "Gabinete"},
     "phase": "A"}

    # edges/*.ndjson
    {"type": "SEGUIDA_POR",
     "from": {"label": "Atividade", "key": {"source_id": "123"}},
     "to":   {"label": "Atividade", "key": {"source_id": "124"}},
     "props": {"intervalo_horas": 4.2},
     "phase": "C"}

    # templates/*.ndjson
    {"name": "seed_tipos",
     "cypher": "UNWIND $types ...",
     "params": {"types": [...]},
     "phase": "A"}

Thread safety
-------------
A single `threading.Lock` guards file handle access and manifest mutation,
so a single `JsonFileWriter` instance can be shared across the Phase B
`ThreadPoolExecutor` workers just like `DirectNeo4jWriter`.
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Iterable, Mapping, TextIO

from .base import GraphWriter, WriterError

log = logging.getLogger(__name__)

_SCHEMA_VERSION = 1
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_NON_SHARD_CHAR = re.compile(r"[^a-z0-9_]")


def _to_shard_name(name: str) -> str:
    """Convert a label or rel type to a safe NDJSON filename stem.

    `GrupoAtividade`            -> `grupo_atividade`
    `PERTENCE_AO_ORGAO`          -> `pertence_ao_orgao`
    `SEGUIDO_INDEPENDENTEMENTE_POR` -> `seguido_independentemente_por`
    """
    snake = _CAMEL_BOUNDARY.sub("_", name).lower()
    return _NON_SHARD_CHAR.sub("_", snake)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class JsonFileWriter(GraphWriter):
    def __init__(self, output_dir: Path | str) -> None:
        self._output_dir = Path(output_dir)
        (self._output_dir / "nodes").mkdir(parents=True, exist_ok=True)
        (self._output_dir / "edges").mkdir(parents=True, exist_ok=True)
        (self._output_dir / "templates").mkdir(parents=True, exist_ok=True)

        self._handles: dict[Path, TextIO] = {}
        self._lock = Lock()
        self._manifest: dict[str, Any] = {
            "schema_version": _SCHEMA_VERSION,
            "output_dir": str(self._output_dir),
            "started_at": _now_iso(),
            "completed_at": None,
            "phases_in_progress": [],
            "phases_completed": [],
            "nodes_counts": {},
            "edges_counts": {},
            "template_counts": {},
        }
        self._write_manifest_locked()

    # -- Lifecycle ---------------------------------------------------------

    def open_phase(self, phase: str) -> None:
        with self._lock:
            if phase not in self._manifest["phases_in_progress"]:
                self._manifest["phases_in_progress"].append(phase)
            self._write_manifest_locked()
        log.info("JsonFileWriter: phase %s opened", phase)

    def close_phase(self, phase: str) -> None:
        with self._lock:
            for fh in self._handles.values():
                fh.flush()
                try:
                    os.fsync(fh.fileno())
                except OSError:
                    pass  # not all filesystems support fsync
            if phase in self._manifest["phases_in_progress"]:
                self._manifest["phases_in_progress"].remove(phase)
            if phase not in self._manifest["phases_completed"]:
                self._manifest["phases_completed"].append(phase)
            self._write_manifest_locked()
        log.info("JsonFileWriter: phase %s closed", phase)

    def close(self) -> None:
        with self._lock:
            for fh in self._handles.values():
                try:
                    fh.flush()
                    os.fsync(fh.fileno())
                except OSError:
                    pass
                fh.close()
            self._handles.clear()
            self._manifest["completed_at"] = _now_iso()
            self._write_manifest_locked()

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
        data = [dict(r) for r in rows]
        if not data:
            return 0
        if not key_fields:
            raise WriterError(f"write_nodes({label}) requires at least one key field")

        key_set = set(key_fields)
        if props is None:
            prop_fields = [k for k in data[0].keys() if k not in key_set]
        else:
            prop_fields = list(props)

        path = self._output_dir / "nodes" / f"{_to_shard_name(label)}.ndjson"
        with self._lock:
            fh = self._handle_locked(path)
            for row in data:
                line = {
                    "label": label,
                    "key": {k: row.get(k) for k in key_fields},
                    "props": {k: row.get(k) for k in prop_fields},
                    "phase": phase,
                }
                fh.write(json.dumps(line, ensure_ascii=False, default=str))
                fh.write("\n")
            counts = self._manifest["nodes_counts"]
            counts[label] = counts.get(label, 0) + len(data)
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
        data = [dict(r) for r in rows]
        if not data:
            return 0
        if not from_key or not to_key:
            raise WriterError(f"write_edges({rel_type}) requires non-empty from_key and to_key")

        prop_fields = list(props) if props else []
        path = self._output_dir / "edges" / f"{_to_shard_name(rel_type)}.ndjson"
        with self._lock:
            fh = self._handle_locked(path)
            for row in data:
                line = {
                    "type": rel_type,
                    "from": {
                        "label": from_label,
                        "key": {node_prop: row.get(row_field)
                                for row_field, node_prop in from_key.items()},
                    },
                    "to": {
                        "label": to_label,
                        "key": {node_prop: row.get(row_field)
                                for row_field, node_prop in to_key.items()},
                    },
                    "props": {k: row.get(k) for k in prop_fields},
                    "phase": phase,
                }
                fh.write(json.dumps(line, ensure_ascii=False, default=str))
                fh.write("\n")
            counts = self._manifest["edges_counts"]
            counts[rel_type] = counts.get(rel_type, 0) + len(data)
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
        path = self._output_dir / "templates" / f"{_to_shard_name(name)}.ndjson"
        with self._lock:
            fh = self._handle_locked(path)
            line = {
                "name": name,
                "cypher": cypher,
                "params": dict(params),
                "phase": phase,
            }
            fh.write(json.dumps(line, ensure_ascii=False, default=str))
            fh.write("\n")
            counts = self._manifest["template_counts"]
            counts[name] = counts.get(name, 0) + 1
        return 1

    # -- Internals ---------------------------------------------------------

    def _handle_locked(self, path: Path) -> TextIO:
        fh = self._handles.get(path)
        if fh is None:
            fh = path.open("a", encoding="utf-8")
            self._handles[path] = fh
        return fh

    def _write_manifest_locked(self) -> None:
        manifest_path = self._output_dir / "manifest.json"
        manifest_path.write_text(
            json.dumps(self._manifest, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
