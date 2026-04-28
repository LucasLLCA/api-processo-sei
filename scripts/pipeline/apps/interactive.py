"""``pipeline`` (no args) — interactive wizard.

Uses ``questionary`` for multi-select / select / text prompts and the
existing ``runner`` for execution. Picks up where ``status`` leaves off:
already-completed stages are pre-marked so the user can quickly re-run only
what's pending.
"""

from __future__ import annotations

from pathlib import Path

import questionary
import typer
from rich.console import Console

from .. import runner
from .. import stages  # noqa: F401
from ..config import Settings
from ..logging_setup import configure_logging
from ..registry import all_stages, resolve
from ..state import StateDir

console = Console()


def _gather_stages_choices(state: StateDir) -> list[questionary.Choice]:
    records = state.all_records()
    choices: list[questionary.Choice] = []
    type_order = {"bootstrap": 0, "core": 1, "enrich": 2, "op": 3}
    for s in sorted(all_stages(), key=lambda x: (type_order.get(x.meta.type, 99), x.meta.name)):
        rec = records.get(s.meta.name)
        suffix = ""
        if rec and rec.is_complete:
            suffix = f"  ✓ done {rec.completed_at[:10]}"
        elif rec and rec.failed_at:
            suffix = f"  ✗ failed {rec.failed_at[:10]}"
        title = f"{s.meta.name:18s} [{s.meta.type}]{suffix}"
        choices.append(questionary.Choice(title=title, value=s.meta.name))
    return choices


def _common_modes_for(stage_names: list[str]) -> list[str]:
    """Intersection of supported modes across all selected stages."""
    plan = resolve(stage_names)
    if not plan:
        return []
    common: set[str] = set(plan[0].meta.modes)
    for s in plan[1:]:
        common &= set(s.meta.modes)
    return sorted(common)


def register(app: typer.Typer) -> None:
    @app.command("interactive", hidden=True)
    @app.callback(invoke_without_command=True)
    def interactive_cmd(ctx: typer.Context) -> None:
        """Interactive wizard (default when no subcommand is given)."""
        if ctx.invoked_subcommand is not None:
            return
        return _run_wizard()


def _run_wizard() -> None:
    console.print("[bold blue]Pipeline — modo interativo[/bold blue]")
    console.print("Use ↑↓ pra navegar, espaço pra marcar, Enter pra confirmar.\n")

    state = StateDir(Path("./.pipeline-state"))
    state.ensure()

    selected = questionary.checkbox(
        "Quais stages rodar?",
        choices=_gather_stages_choices(state),
    ).ask()
    if not selected:
        console.print("[yellow]Nada selecionado. Saindo.[/yellow]")
        return

    plan = resolve(selected)
    plan_names = [s.meta.name for s in plan]
    if set(plan_names) != set(selected):
        added = set(plan_names) - set(selected)
        console.print(f"[dim]Auto-adicionados (deps): {', '.join(sorted(added))}[/dim]")

    modes = _common_modes_for(selected)
    if not modes:
        console.print("[red]Os stages selecionados não têm modo em comum. Reduza a seleção.[/red]")
        return
    mode = questionary.select("Modo de execução?", choices=modes).ask()
    if mode is None:
        return

    flags: dict = {}

    emit_dir = read_dir = None
    if mode in ("json-emit", "json-replay"):
        if mode == "json-emit":
            emit_dir = questionary.text("Emit dir?", default="./graphify-out").ask()
            flags["emit_dir"] = emit_dir
        if mode == "json-replay":
            read_dir = questionary.text("Read dir?", default="./graphify-out").ask()
            flags["read_dir"] = read_dir

    needs_dates = any(s.meta.name in ("precreate", "atividades", "etl") for s in plan)
    if needs_dates:
        from_date = questionary.text("Filtrar a partir de (--from, YYYY-MM-DD)? [vazio para sem filtro]", default="").ask()
        to_date = questionary.text("Filtrar até (--to)? [vazio para sem filtro]", default="").ask()
        if from_date:
            flags["from_date"] = from_date
        if to_date:
            flags["to_date"] = to_date
        workers = questionary.text("Workers? (paralelismo da fase atividades)", default="8").ask()
        if workers:
            try:
                flags["workers"] = int(workers)
            except ValueError:
                pass

    force = questionary.confirm("Forçar re-execução mesmo se já completado?", default=False).ask()

    console.print("\n[bold]Plano:[/bold]")
    for i, s in enumerate(plan, 1):
        console.print(f"  {i:2d}. {s.meta.name}  ({s.meta.estimated_duration or 'duration:?'})")
    console.print(f"  Mode: [cyan]{mode}[/cyan]")
    console.print(f"  Force: {force}")

    if not questionary.confirm("Confirmar execução?", default=True).ask():
        console.print("[yellow]Cancelado.[/yellow]")
        return

    settings = Settings.from_env()
    configure_logging("pipeline", settings.log_level)

    runner.run_stages(
        selected,
        settings=settings,
        mode=mode,
        flags=flags,
        force=force,
        console=console,
    )
