"""Stage ``clean`` — wipe graph state.

In ``neo4j`` mode: delete all relationships, nodes, constraints and indexes
in batches via the live Neo4j driver.

In ``json-emit`` mode: ``shutil.rmtree`` the emit directory passed via
``--emit-dir`` (or ``settings.emit_json_dir``). The replay step would
otherwise carry forward stale NDJSON between runs.

Usage (standalone):
    python scripts/pipeline/stages/clean.py              # interactive confirm
    python scripts/pipeline/stages/clean.py --force      # skip confirm
"""

from __future__ import annotations

import argparse
import shutil
import sys as _sys
from pathlib import Path

_HERE = Path(__file__).resolve()
_SCRIPTS = next(p for p in _HERE.parents if p.name == "scripts")
for _p in (_SCRIPTS, _SCRIPTS.parent):
    if str(_p) not in _sys.path:
        _sys.path.insert(0, str(_p))

from pipeline.cli import add_standard_args, resolve_settings
from pipeline.logging_setup import configure_logging
from pipeline.neo4j_driver import build_driver
from pipeline.registry import stage
from pipeline._stage_base import RunContext, StageMeta

log = configure_logging(__name__)


# ---------------------------------------------------------------------------
# Neo4j cleanup
# ---------------------------------------------------------------------------
def clean_neo4j(driver) -> dict:
    """Delete all nodes and relationships in batches, then drop schema."""
    with driver.session() as session:
        result = session.run(
            "MATCH (n) RETURN count(n) AS nodes, "
            "size([(n)-[r]-() | r]) AS rels"
        )
        rec = result.single()
    total_nodes = rec["nodes"]
    log.info("Found %d nodes to delete", total_nodes)

    deleted_rels = 0
    while True:
        with driver.session() as session:
            result = session.run(
                "MATCH ()-[r]->() WITH r LIMIT 5000 DELETE r RETURN count(*) AS cnt"
            )
            batch = result.single()["cnt"]
        if batch == 0:
            break
        deleted_rels += batch
        if deleted_rels % 50000 == 0 or batch < 5000:
            log.info("  Deleted %d relationships", deleted_rels)

    log.info("All %d relationships deleted", deleted_rels)

    deleted_nodes = 0
    while True:
        with driver.session() as session:
            result = session.run(
                "MATCH (n) WITH n LIMIT 10000 DELETE n RETURN count(*) AS cnt"
            )
            batch = result.single()["cnt"]
        if batch == 0:
            break
        deleted_nodes += batch
        if deleted_nodes % 50000 == 0 or batch < 10000:
            log.info("  Deleted %d / %d nodes", deleted_nodes, total_nodes)

    log.info("All %d nodes deleted", deleted_nodes)

    with driver.session() as session:
        result = session.run("SHOW CONSTRAINTS")
        constraints = [r["name"] for r in result]
    for name in constraints:
        with driver.session() as session:
            session.run(f"DROP CONSTRAINT {name} IF EXISTS")
        log.info("  Dropped constraint: %s", name)

    with driver.session() as session:
        result = session.run("SHOW INDEXES")
        indexes = [r["name"] for r in result if r["type"] != "LOOKUP"]
    for name in indexes:
        with driver.session() as session:
            session.run(f"DROP INDEX {name} IF EXISTS")
        log.info("  Dropped index: %s", name)

    log.info("Cleanup complete")
    return {
        "deleted_nodes": deleted_nodes,
        "deleted_rels": deleted_rels,
        "dropped_constraints": len(constraints),
        "dropped_indexes": len(indexes),
    }


# Back-compat alias for old name.
clean = clean_neo4j


# ---------------------------------------------------------------------------
# JSON emit-dir cleanup
# ---------------------------------------------------------------------------
def clean_json_dir(emit_dir: Path) -> dict:
    if not emit_dir.exists():
        log.info("Emit directory %s does not exist — nothing to clean.", emit_dir)
        return {"deleted_dir": False, "path": str(emit_dir)}
    shutil.rmtree(emit_dir)
    log.info("Removed emit directory: %s", emit_dir)
    return {"deleted_dir": True, "path": str(emit_dir)}


# ---------------------------------------------------------------------------
# Standalone entry
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Clean graph state (Neo4j or emit-dir)")
    parser.add_argument("--force", action="store_true", help="Skip confirmation")
    add_standard_args(parser)
    args = parser.parse_args()

    settings = resolve_settings(args)
    configure_logging(__name__, settings.log_level)

    if settings.emit_json_dir is not None:
        target = Path(settings.emit_json_dir)
        if not args.force:
            answer = input(f"This will DELETE {target}. Continue? [y/N] ")
            if answer.lower() != "y":
                log.info("Aborted")
                return
        clean_json_dir(target)
        return

    driver = build_driver(settings)
    log.info("Connected to Neo4j: %s", settings.neo4j_uri)
    try:
        if not args.force:
            answer = input("This will DELETE ALL DATA in Neo4j. Continue? [y/N] ")
            if answer.lower() != "y":
                log.info("Aborted")
                return
        clean_neo4j(driver)
    finally:
        driver.close()


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------
@stage(StageMeta(
    name="clean",
    description="Apaga estado do grafo (Neo4j) ou diretório de emit JSON.",
    type="op",
    depends_on=(),
    modes=("neo4j", "json-emit"),
    can_skip_when_done=False,  # always runs when invoked
    estimated_duration="<1min",
))
def run(ctx: RunContext) -> None:
    if ctx.mode == "json-emit":
        emit_dir = Path(ctx.flags.get("emit_dir") or ctx.settings.emit_json_dir or "./graphify-out")
        ctx.cache["clean_summary"] = clean_json_dir(emit_dir)
        return

    driver = ctx.require_driver()
    ctx.cache["clean_summary"] = clean_neo4j(driver)


if __name__ == "__main__":
    main()
