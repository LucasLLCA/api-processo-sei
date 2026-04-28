"""Lazy-loaded bootstrap stages.

These stages depend on ``api.*`` (FastAPI app, SQLAlchemy models). To keep
the pipeline package importable in minimal environments, the actual
runners live in ``scripts/bootstrap/`` and we only register *stubs* here
that import lazily on first invocation.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

from ..registry import stage
from .._stage_base import RunContext, StageMeta


def _ensure_scripts_on_path() -> None:
    """Add the project's ``scripts/`` dir to sys.path so ``bootstrap`` resolves."""
    here = Path(__file__).resolve()
    scripts_dir = next(p for p in here.parents if p.name == "scripts")
    project = scripts_dir.parent
    for p in (scripts_dir, project):
        s = str(p)
        if s not in sys.path:
            sys.path.insert(0, s)


@stage(StageMeta(
    name="unidades",
    description="Popula PostgreSQL.unidades_sei a partir do CSV (acopla api.database).",
    type="bootstrap",
    depends_on=(),
    modes=("postgres",),
    estimated_duration="<30s",
))
def run_unidades(ctx: RunContext) -> None:
    _ensure_scripts_on_path()
    mod = importlib.import_module("bootstrap.populate_unidades_sei")
    # The script's main() reads sys.argv. Synthesize argv from ctx.flags.
    csv_path = ctx.flags.get("csv_path")
    batch_size = ctx.flags.get("batch_size") or 500
    argv = ["populate_unidades_sei"]
    if csv_path:
        argv += ["--csv-path", str(csv_path)]
    argv += ["--batch-size", str(batch_size)]
    saved = sys.argv
    try:
        sys.argv = argv
        mod.main()
    finally:
        sys.argv = saved
    ctx.cache["unidades_summary"] = {"csv_path": str(csv_path) if csv_path else None}


@stage(StageMeta(
    name="tipos",
    description="Popula PostgreSQL.tipos_documento via API SEI (acopla api.database).",
    type="bootstrap",
    depends_on=(),
    modes=("postgres",),
    estimated_duration="<2min",
))
def run_tipos(ctx: RunContext) -> None:
    _ensure_scripts_on_path()
    mod = importlib.import_module("bootstrap.populate_tipos_documento")
    argv = ["populate_tipos_documento"]
    for flag in ("usuario", "senha", "orgao"):
        v = ctx.flags.get(flag)
        if v is not None:
            argv += [f"--{flag}", str(v)]
    if ctx.flags.get("dry_run"):
        argv += ["--dry-run"]
    saved = sys.argv
    try:
        sys.argv = argv
        mod.main()
    finally:
        sys.argv = saved
    ctx.cache["tipos_summary"] = {"orgao": ctx.flags.get("orgao", "SEAD-PI")}


@stage(StageMeta(
    name="etl",
    description="Macro: roda precreate + atividades + timeline + permanencia + situacao em ordem.",
    type="core",
    depends_on=("precreate", "atividades", "timeline", "permanencia", "situacao"),
    modes=("neo4j", "json-emit"),
    estimated_duration="varia conforme volume",
))
def run_etl(ctx: RunContext) -> None:
    """No-op: dependências fazem todo o trabalho via topological resolve."""
    return None


@stage(StageMeta(
    name="etl-full",
    description="Macro completo: etl + download + parse + ner + embed + similarity + processo-cluster.",
    type="core",
    depends_on=(
        "etl", "download", "parse", "ner-extract", "ner-load",
        "embed", "similarity", "processo-cluster",
    ),
    modes=("neo4j", "json-emit"),
    estimated_duration="varia significativamente; horas para corpus grande",
))
def run_etl_full(ctx: RunContext) -> None:
    """No-op: o orquestrador resolve toda a árvore de dependências."""
    return None
