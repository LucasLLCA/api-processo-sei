"""``pipeline list`` and ``pipeline graph`` — inspect the stage registry."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

from .. import stages  # noqa: F401
from ..registry import all_stages, resolve

console = Console()


def register(app: typer.Typer) -> None:
    @app.command("list")
    def list_cmd() -> None:
        """List all registered stages with their type, deps and supported modes."""
        table = Table(title="Pipeline Stages", show_lines=False)
        table.add_column("Name", style="bold cyan")
        table.add_column("Type")
        table.add_column("Depends on")
        table.add_column("Modes")
        table.add_column("Description", overflow="fold")

        order = {"bootstrap": 0, "core": 1, "enrich": 2, "op": 3}
        sorted_stages = sorted(all_stages(), key=lambda s: (order.get(s.meta.type, 99), s.meta.name))

        for s in sorted_stages:
            deps = ", ".join(s.meta.depends_on) or "—"
            modes = ", ".join(s.meta.modes) or "—"
            table.add_row(s.meta.name, s.meta.type, deps, modes, s.meta.description)
        console.print(table)

    @app.command("graph")
    def graph_cmd(stages_arg: list[str] = typer.Argument(None)) -> None:
        """Show resolved execution order for one or more stages (or all)."""
        names = stages_arg or [s.meta.name for s in all_stages() if s.meta.type != "bootstrap"]
        plan = resolve(names)
        console.print(f"\n[bold]Topological order for: {', '.join(names)}[/bold]")
        for i, s in enumerate(plan, 1):
            deps = " → " + ", ".join(s.meta.depends_on) if s.meta.depends_on else ""
            console.print(f"  {i:2d}. [cyan]{s.meta.name}[/cyan]  [{s.meta.type}]{deps}")
