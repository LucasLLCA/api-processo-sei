"""``pipeline run STAGE...`` — execute one or more stages with the runner.

Common flags are exposed (mode, dirs, dates, workers, force). Stage-specific
flags can be passed via ``--flag KEY=VALUE`` repeatedly, since each stage's
``run(ctx)`` reads from ``ctx.flags``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from .. import runner
from .. import stages  # noqa: F401  -- registry side-effect import
from ..cli import resolve_settings
from ..logging_setup import configure_logging
from ..registry import StageNotFoundError, IncompatibleModeError

console = Console()


def _make_settings(
    *,
    neo4j_uri: Optional[str],
    neo4j_user: Optional[str],
    neo4j_password: Optional[str],
    neo4j_database: Optional[str],
    emit_dir: Optional[Path],
    read_dir: Optional[Path],
    log_level: Optional[str],
    batch_size: Optional[int],
    workers: Optional[int],
):
    """Build a Settings object reusing the existing argparse-based resolver."""
    import argparse
    ns = argparse.Namespace(
        neo4j_uri=neo4j_uri,
        neo4j_user=neo4j_user,
        neo4j_password=neo4j_password,
        neo4j_database=neo4j_database,
        emit_json_dir=emit_dir,
        read_json_dir=read_dir,
        log_level=log_level,
        batch_size=batch_size,
        workers=workers,
    )
    return resolve_settings(ns)


def _parse_flag(flag: str) -> tuple[str, str]:
    if "=" not in flag:
        raise typer.BadParameter(f"--flag requires KEY=VALUE form, got: {flag!r}")
    k, v = flag.split("=", 1)
    return k.strip(), v.strip()


def register(app: typer.Typer) -> None:
    @app.command("run")
    def run_cmd(
        stages_arg: list[str] = typer.Argument(..., help="Stage names (one or more). Deps auto-resolved."),
        mode: Optional[str] = typer.Option(None, "--mode", help="neo4j | json-emit | json-replay | fs | postgres"),
        emit_dir: Optional[Path] = typer.Option(None, "--emit-dir", "--emit-json"),
        read_dir: Optional[Path] = typer.Option(None, "--read-dir", "--read-json"),
        from_date: Optional[str] = typer.Option(None, "--from"),
        to_date: Optional[str] = typer.Option(None, "--to"),
        workers: Optional[int] = typer.Option(None, "--workers"),
        batch_size: Optional[int] = typer.Option(None, "--batch-size"),
        chunk_size: Optional[int] = typer.Option(None, "--chunk-size"),
        limit: Optional[int] = typer.Option(None, "--limit"),
        force: bool = typer.Option(False, "--force", help="Re-run all selected stages, ignoring checkpoints."),
        force_stage: list[str] = typer.Option([], "--force-stage", help="Re-run only this stage (may repeat)."),
        state_dir: Optional[Path] = typer.Option(None, "--state-dir", help="Override .pipeline-state directory."),
        dry_resolve: bool = typer.Option(False, "--dry-resolve", help="Show plan, don't execute."),
        flag: list[str] = typer.Option([], "--flag", help="Stage-specific flag KEY=VALUE (repeatable)."),
        neo4j_uri: Optional[str] = typer.Option(None, "--neo4j-uri"),
        neo4j_user: Optional[str] = typer.Option(None, "--neo4j-user"),
        neo4j_password: Optional[str] = typer.Option(None, "--neo4j-password"),
        neo4j_database: Optional[str] = typer.Option(None, "--neo4j-database"),
        log_level: Optional[str] = typer.Option(None, "--log-level"),
    ) -> None:
        """Run one or more stages, resolving dependencies in topological order."""
        settings = _make_settings(
            neo4j_uri=neo4j_uri,
            neo4j_user=neo4j_user,
            neo4j_password=neo4j_password,
            neo4j_database=neo4j_database,
            emit_dir=emit_dir,
            read_dir=read_dir,
            log_level=log_level,
            batch_size=batch_size,
            workers=workers,
        )
        configure_logging("pipeline", settings.log_level)

        flags: dict = {
            "from_date": from_date,
            "to_date": to_date,
            "workers": workers,
            "batch_size": batch_size,
            "chunk_size": chunk_size,
            "limit": limit,
            "emit_dir": str(emit_dir) if emit_dir else None,
            "read_dir": str(read_dir) if read_dir else None,
        }
        for f in flag:
            k, v = _parse_flag(f)
            flags[k] = v

        try:
            runner.run_stages(
                stages_arg,
                settings=settings,
                mode=mode,
                flags=flags,
                state_dir=state_dir,
                force=force,
                force_stage=set(force_stage),
                dry_resolve=dry_resolve,
                console=console,
            )
        except StageNotFoundError as e:
            console.print(f"[red]Unknown stage: {e}[/red]")
            console.print("Use [bold]pipeline list[/bold] to see available stages.")
            raise typer.Exit(2)
        except IncompatibleModeError as e:
            console.print(f"[red]{e}[/red]")
            raise typer.Exit(2)
