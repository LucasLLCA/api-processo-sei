"""Pipeline runner — execute resolved stages with mode-aware I/O + checkpoints.

The runner takes a list of stage names, resolves dependencies, sets up the
appropriate writer/reader/driver based on ``mode``, and dispatches each
stage in order. Completed stages are recorded in the state-dir; subsequent
runs skip them unless ``force=True``.

Used both by ``pipeline run`` (Typer) and by the legacy ``pipeline.etl``
shim.
"""

from __future__ import annotations

import logging
import time
import traceback
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)

from ._stage_base import Mode, RunContext
from .config import ConfigError, Settings
from .neo4j_driver import build_driver
from .readers import GraphReader, JsonFileReader, Neo4jReader
from .registry import (
    IncompatibleModeError,
    StageDef,
    check_mode,
    resolve,
    soft_dep_warnings,
)
from .state import StageRecord, StateDir, args_compatible, now_iso
from .writers import DirectNeo4jWriter, GraphWriter, JsonFileWriter

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Mode helpers
# ---------------------------------------------------------------------------
def pick_mode(settings: Settings, args: dict | None = None) -> Mode:
    """Heuristic: derive mode from settings/flags when caller didn't pin one.

    - ``--read-json DIR``  → ``json-replay``
    - ``--emit-json DIR``  → ``json-emit``
    - default              → ``neo4j``
    """
    args = args or {}
    if args.get("mode"):
        return args["mode"]  # explicit
    if settings.read_json_dir is not None or args.get("read_dir"):
        return "json-replay"
    if settings.emit_json_dir is not None or args.get("emit_dir"):
        return "json-emit"
    return "neo4j"


# ---------------------------------------------------------------------------
# IO setup / teardown
# ---------------------------------------------------------------------------
@contextmanager
def _io_for(mode: Mode, settings: Settings, args: dict):
    """Open writer/reader/driver for the given mode and yield them.

    Caller passes the result into ``RunContext``. The context manager closes
    everything on exit so each runner invocation is self-contained.
    """
    writer: GraphWriter | None = None
    reader: GraphReader | None = None
    driver = None
    own_driver = False

    emit_dir = args.get("emit_dir") or settings.emit_json_dir
    read_dir = args.get("read_dir") or settings.read_json_dir

    try:
        if mode == "neo4j":
            try:
                driver = build_driver(settings)
                own_driver = True
            except ConfigError as e:
                raise IncompatibleModeError(
                    f"mode=neo4j needs Neo4j credentials: {e}"
                ) from e
            batch_size = int(args.get("batch_size") or settings.batch_size or 1000)
            writer = DirectNeo4jWriter(driver, batch_size=batch_size)
            reader = Neo4jReader(driver)

        elif mode == "json-emit":
            target = Path(emit_dir or "./graphify-out")
            target.mkdir(parents=True, exist_ok=True)
            writer = JsonFileWriter(target)
            # Best-effort reader for stages that need it (timeline / permanência):
            # try to bring up Neo4j; if unavailable, leave reader=None.
            try:
                driver = build_driver(settings)
                own_driver = True
                reader = Neo4jReader(driver)
            except ConfigError:
                log.info("json-emit: Neo4j not configured; reader-side stages will fail if requested.")

        elif mode == "json-replay":
            if not read_dir:
                raise IncompatibleModeError("mode=json-replay requires --read-dir")
            reader = JsonFileReader(Path(read_dir))
            # Write side is best-effort: if --emit-dir is given, write NDJSON;
            # otherwise try to bring up Neo4j; if neither is available, leave
            # writer=None. Read-only stages (resumo) work fine with no writer;
            # stages that DO need a writer will fail via ctx.require_writer().
            if emit_dir:
                writer = JsonFileWriter(Path(emit_dir))
            else:
                try:
                    driver = build_driver(settings)
                    own_driver = True
                    writer = DirectNeo4jWriter(driver, batch_size=int(args.get("batch_size") or 1000))
                except ConfigError:
                    log.info("json-replay: no --emit-dir and no Neo4j credentials; "
                             "writer-side stages will fail if requested.")

        elif mode == "fs":
            pass  # filesystem-only stages need none of writer/reader/driver

        elif mode == "postgres":
            pass  # bootstrap stages handle their own DB connections

        else:
            raise IncompatibleModeError(f"unknown mode: {mode!r}")

        yield writer, reader, driver

    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass
        if reader is not None:
            try:
                reader.close()
            except Exception:
                pass
        if own_driver and driver is not None:
            try:
                driver.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
def run_stages(
    stage_names: list[str],
    *,
    settings: Settings,
    mode: Mode | None = None,
    flags: dict | None = None,
    state_dir: Path | None = None,
    force: bool = False,
    force_stage: set[str] | None = None,
    dry_resolve: bool = False,
    yes: bool = True,
    console: Console | None = None,
) -> dict[str, Any]:
    """Run the given stages in topological order with checkpoints + progress.

    Returns a summary dict with per-stage outcomes (``ran``, ``skipped``,
    ``failed``).
    """
    flags = flags or {}
    force_stage = force_stage or set()
    console = console or Console()

    # ── Resolve dependencies ──
    plan: list[StageDef] = resolve(stage_names)

    # Determine the user-preferred graph mode (early so we can validate).
    # Each stage picks its own actual mode at run time: if the user's mode is
    # in the stage's `modes` set we use it; otherwise we fall back to the
    # stage's first declared mode (typical case: fs-only stages always
    # run in 'fs' regardless of the global --mode).
    chosen_mode: Mode = mode or pick_mode(settings, flags)

    def _resolved_mode_for(s: StageDef) -> Mode:
        if chosen_mode in s.meta.modes:
            return chosen_mode
        if s.meta.modes:
            return s.meta.modes[0]
        raise IncompatibleModeError(
            f"stage {s.meta.name!r} declares no modes — cannot run."
        )

    warnings = soft_dep_warnings(plan)
    for stage_name, missing in warnings:
        log.warning(
            "stage %s suggests soft-dep %s (not in selection)",
            stage_name, missing,
        )

    # ── State dir ──
    sdir = StateDir(state_dir or Path("./.pipeline-state"))
    sdir.ensure()
    if not sdir.try_lock():
        raise RuntimeError(
            f"Another pipeline run holds the lock at {sdir.lock_file}. "
            "Wait for it to finish or remove the lock manually."
        )

    summary: dict[str, dict] = {}

    if dry_resolve:
        console.print(f"\n[bold]Dry resolve (preferred mode={chosen_mode}):[/bold]")
        for s in plan:
            rec = sdir.read_stage(s.meta.name)
            status = "[green]done[/green]" if rec and rec.is_complete else "[yellow]pending[/yellow]"
            actual = _resolved_mode_for(s)
            console.print(f"  {s.meta.name:15s}  {status}  mode={actual:11s} {s.meta.description}")
        sdir.release_lock()
        return {"resolved": [s.meta.name for s in plan], "mode": chosen_mode}

    run_started = now_iso()
    run_started_t = time.time()

    # Shared cache survives across IO contexts so discovery (Postgres) computed
    # by precreate is reused by atividades inside the same run.
    shared_cache: dict[str, Any] = {}

    try:
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console,
            transient=False,
        ) as progress:
            outer = progress.add_task("Pipeline", total=len(plan))

            for s in plan:
                name = s.meta.name
                # Checkpoint check
                rec = sdir.read_stage(name)
                if (
                    rec
                    and rec.is_complete
                    and s.meta.can_skip_when_done
                    and not force
                    and name not in force_stage
                    and args_compatible(rec.args, flags)
                ):
                    log.info("[skip] %s already complete at %s (mode=%s)",
                             name, rec.completed_at, rec.mode)
                    summary[name] = {"status": "skipped", "completed_at": rec.completed_at}
                    progress.advance(outer)
                    continue

                stage_mode = _resolved_mode_for(s)
                progress.update(outer, description=f"Pipeline → {name}")
                inner = progress.add_task(f"  {name}", total=None)
                started_at = now_iso()
                started_t = time.time()
                log.info(">>> stage %s (mode=%s)", name, stage_mode)

                record = StageRecord(
                    stage=name,
                    started_at=started_at,
                    mode=stage_mode,
                    args=dict(flags),
                )
                try:
                    with _io_for(stage_mode, settings, flags) as (writer, reader, driver):
                        ctx = RunContext(
                            settings=settings,
                            mode=stage_mode,
                            flags=flags,
                            state_dir=sdir.path,
                            cache=shared_cache,
                            writer=writer,
                            reader=reader,
                            driver=driver,
                        )
                        s.call(ctx)
                    elapsed = time.time() - started_t
                    record.completed_at = now_iso()
                    record.summary = shared_cache.get(f"{name.replace('-', '_')}_summary", {})
                    record.summary["duration_seconds"] = round(elapsed, 1)
                    sdir.write_stage(record)
                    summary[name] = {"status": "ran", "duration_seconds": elapsed, "mode": stage_mode}
                    log.info("<<< stage %s OK in %.1fs", name, elapsed)
                except Exception as e:
                    elapsed = time.time() - started_t
                    record.failed_at = now_iso()
                    record.error = f"{type(e).__name__}: {e}"
                    record.summary = {"traceback": traceback.format_exc(), "duration_seconds": round(elapsed, 1)}
                    sdir.write_stage(record)
                    summary[name] = {"status": "failed", "error": str(e), "mode": stage_mode}
                    log.exception("<<< stage %s FAILED in %.1fs", name, elapsed)
                    raise
                finally:
                    progress.remove_task(inner)
                    progress.advance(outer)

    finally:
        run_ended = now_iso()
        sdir.append_run({
            "started": run_started,
            "ended": run_ended,
            "duration_s": round(time.time() - run_started_t, 1),
            "stages": [s.meta.name for s in plan],
            "mode": chosen_mode,
            "force": force,
            "force_stage": sorted(force_stage),
            "outcome": summary,
        })
        sdir.release_lock()

    return {"resolved": [s.meta.name for s in plan], "mode": chosen_mode, "stages": summary}
