"""Replay a JsonFileWriter emit directory into a live Neo4j database.

Walks the NDJSON shards + templates produced by `JsonFileWriter`, re-hydrates
each row, and routes everything through a `DirectNeo4jWriter` so the target
database ends up bit-identical to what the original `etl_neo4j.py --emit-json`
run would have produced against Neo4j directly.

Usage::

    python -m pipeline.replay --input /tmp/emit
    python scripts/pipeline/replay.py --input /tmp/emit --neo4j-uri bolt://... --neo4j-password ...
    python -m pipeline.replay --input /tmp/emit --phase A --phase B   # partial replay

Within each phase, files are replayed in this order:
    1. `nodes/*.ndjson`     (must exist before templates/edges MATCH them)
    2. `templates/*.ndjson` (composite MERGE/MATCH statements)
    3. `edges/*.ndjson`     (MATCH-based edge creation)

Phases themselves are replayed in canonical order: schema → A → B → C → D → gliner.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator

# Allow running as `python scripts/pipeline/replay.py` by adding scripts/ to sys.path
_SCRIPTS_DIR = Path(__file__).resolve().parent.parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from pipeline.cli import add_standard_args, resolve_settings
from pipeline.config import ConfigError
from pipeline.logging_setup import configure_logging
from pipeline.neo4j_driver import build_driver
from pipeline.writers import DirectNeo4jWriter, GraphWriter

log = configure_logging(__name__)

# Canonical phase order. Phases not present in the emit dir are silently skipped.
_PHASE_ORDER: tuple[str, ...] = ("schema", "A", "B", "C", "D", "gliner")


# ---------------------------------------------------------------------------
# NDJSON streaming
# ---------------------------------------------------------------------------
def _read_ndjson_lines(path: Path) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


# ---------------------------------------------------------------------------
# Per-shard replay
# ---------------------------------------------------------------------------
def _replay_nodes_file(writer: GraphWriter, path: Path, phase: str) -> int:
    """Re-hydrate a nodes/*.ndjson shard into `write_nodes` calls.

    Lines are grouped by (key_fields, prop_fields) schema so rows that share
    a shape get batched into one UNWIND statement.
    """
    groups: dict[tuple[str, tuple[str, ...], tuple[str, ...]], list[dict]] = defaultdict(list)
    for line in _read_ndjson_lines(path):
        if line.get("phase") != phase:
            continue
        label = line["label"]
        key_fields = tuple(sorted(line["key"].keys()))
        prop_fields = tuple(sorted((line.get("props") or {}).keys()))
        groups[(label, key_fields, prop_fields)].append(line)

    total = 0
    for (label, key_tuple, prop_tuple), lines in groups.items():
        rows = [
            {**ln["key"], **(ln.get("props") or {})}
            for ln in lines
        ]
        writer.write_nodes(
            label,
            list(key_tuple),
            rows,
            phase=phase,
            props=list(prop_tuple),
        )
        total += len(rows)
    return total


def _replay_edges_file(writer: GraphWriter, path: Path, phase: str) -> int:
    """Re-hydrate an edges/*.ndjson shard into `write_edges` calls.

    NDJSON lines store keys under node-property names (e.g. `{"sigla": "X"}`).
    To pass them to `write_edges` we synthesize row field names by prefixing
    the property with `from__` / `to__`, keeping the two endpoints disambiguated.
    """
    groups: dict[
        tuple[str, str, str, tuple[str, ...], tuple[str, ...], tuple[str, ...]],
        list[dict],
    ] = defaultdict(list)

    for line in _read_ndjson_lines(path):
        if line.get("phase") != phase:
            continue
        rel_type = line["type"]
        from_label = line["from"]["label"]
        to_label = line["to"]["label"]
        from_keys = tuple(sorted(line["from"]["key"].keys()))
        to_keys = tuple(sorted(line["to"]["key"].keys()))
        prop_fields = tuple(sorted((line.get("props") or {}).keys()))
        groups[(rel_type, from_label, to_label, from_keys, to_keys, prop_fields)].append(line)

    total = 0
    for (rel_type, from_label, to_label, from_keys, to_keys, prop_fields), lines in groups.items():
        from_key_map = {f"from__{p}": p for p in from_keys}
        to_key_map = {f"to__{p}": p for p in to_keys}

        rows: list[dict[str, Any]] = []
        for ln in lines:
            row: dict[str, Any] = {}
            for prop in from_keys:
                row[f"from__{prop}"] = ln["from"]["key"][prop]
            for prop in to_keys:
                row[f"to__{prop}"] = ln["to"]["key"][prop]
            line_props = ln.get("props") or {}
            for prop in prop_fields:
                row[prop] = line_props.get(prop)
            rows.append(row)

        writer.write_edges(
            rel_type,
            from_label, from_key_map,
            to_label, to_key_map,
            rows,
            phase=phase,
            props=list(prop_fields) or None,
        )
        total += len(rows)
    return total


def _replay_templates_file(writer: GraphWriter, path: Path, phase: str) -> int:
    """Re-issue every template line as an `execute_template` call."""
    total = 0
    for line in _read_ndjson_lines(path):
        if line.get("phase") != phase:
            continue
        writer.execute_template(
            line["name"],
            line["cypher"],
            line.get("params") or {},
            phase=phase,
        )
        total += 1
    return total


# ---------------------------------------------------------------------------
# Directory walker
# ---------------------------------------------------------------------------
def replay_emit_dir(
    writer: GraphWriter,
    emit_dir: Path,
    phases: list[str],
) -> dict[str, int]:
    """Walk `emit_dir` and replay every shard, phase by phase."""
    counts = {"nodes": 0, "edges": 0, "templates": 0}
    nodes_dir = emit_dir / "nodes"
    edges_dir = emit_dir / "edges"
    tpl_dir = emit_dir / "templates"

    for phase in phases:
        log.info("Replaying phase %s", phase)
        writer.open_phase(phase)
        try:
            # 1. Nodes
            if nodes_dir.is_dir():
                for shard in sorted(nodes_dir.glob("*.ndjson")):
                    n = _replay_nodes_file(writer, shard, phase)
                    if n:
                        log.info("  nodes/%s: %d", shard.name, n)
                        counts["nodes"] += n

            # 2. Templates (composite node+edge statements)
            if tpl_dir.is_dir():
                for shard in sorted(tpl_dir.glob("*.ndjson")):
                    n = _replay_templates_file(writer, shard, phase)
                    if n:
                        log.info("  templates/%s: %d", shard.name, n)
                        counts["templates"] += n

            # 3. Edges (MATCH-dependent on nodes + templates)
            if edges_dir.is_dir():
                for shard in sorted(edges_dir.glob("*.ndjson")):
                    n = _replay_edges_file(writer, shard, phase)
                    if n:
                        log.info("  edges/%s: %d", shard.name, n)
                        counts["edges"] += n
        finally:
            writer.close_phase(phase)

    return counts


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay a graphify emit directory into a live Neo4j instance.",
    )
    parser.add_argument("--input", type=Path, required=True,
                        help="Emit directory produced by etl_neo4j.py --emit-json")
    parser.add_argument("--phase", action="append", default=None,
                        help="Replay only the named phase (repeatable). "
                             "Default: all phases in canonical order.")
    # Replay emits direct to Neo4j: --emit-json / --read-json don't apply,
    # and --workers isn't used here.
    add_standard_args(parser, skip={"--emit-json", "--read-json", "--workers"})
    args = parser.parse_args()

    settings = resolve_settings(args)
    configure_logging(__name__, settings.log_level)

    emit_dir: Path = args.input
    if not emit_dir.is_dir():
        log.error("Emit directory not found: %s", emit_dir)
        sys.exit(1)

    manifest_path = emit_dir / "manifest.json"
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            log.info(
                "Replaying %s (started_at=%s, phases_completed=%s)",
                emit_dir,
                manifest.get("started_at"),
                manifest.get("phases_completed"),
            )
        except json.JSONDecodeError:
            log.warning("manifest.json is not valid JSON at %s; continuing", manifest_path)
    else:
        log.warning("No manifest.json at %s; continuing anyway", manifest_path)

    phases = args.phase if args.phase else list(_PHASE_ORDER)
    log.info("Will replay phases: %s", phases)

    try:
        driver = build_driver(settings)
    except ConfigError as e:
        log.error("%s", e)
        sys.exit(2)
    log.info("Connected to Neo4j: %s", settings.neo4j_uri)

    writer = DirectNeo4jWriter(driver, batch_size=settings.batch_size or 1000)
    try:
        counts = replay_emit_dir(writer, emit_dir, phases)
    finally:
        writer.close()
        driver.close()

    log.info(
        "Replay complete: %d nodes, %d edges, %d template invocations",
        counts["nodes"], counts["edges"], counts["templates"],
    )


if __name__ == "__main__":
    main()
