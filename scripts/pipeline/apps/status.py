"""``pipeline status`` — show graph + filesystem + checkpoint state."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from .. import stages  # noqa: F401
from ..config import ConfigError, Settings
from ..registry import all_stages
from ..state import StateDir

console = Console()


def _safe_count(path: Path, glob: str = "*", recursive: bool = False) -> int:
    if not path.is_dir():
        return -1
    if recursive:
        return sum(1 for _ in path.rglob(glob) if _.is_file())
    return sum(1 for _ in path.glob(glob) if _.is_file())


def _filesystem_table() -> Table:
    table = Table(title="Filesystem", show_header=False, box=None)
    table.add_column("dir", style="cyan")
    table.add_column("count", justify="right")
    paths = [
        ("./documentos_sead/  (raw downloads)", _safe_count(Path("./documentos_sead"), "*", recursive=True)),
        ("./parsed_documents/  (.txt)", _safe_count(Path("./parsed_documents"), "*.txt")),
        ("./parsed_documents/  (.json)", _safe_count(Path("./parsed_documents"), "*.json")),
        ("./ner_results/  (.json)", _safe_count(Path("./ner_results"), "*.json")),
        ("./graphify-out/  (emit-dir)", _safe_count(Path("./graphify-out"), "*", recursive=True)),
    ]
    for label, n in paths:
        table.add_row(label, "—" if n < 0 else f"{n:,}")
    return table


def _neo4j_table(settings: Settings) -> Optional[Table]:
    try:
        from ..neo4j_driver import build_driver
        driver = build_driver(settings)
    except ConfigError:
        return None
    except Exception as e:
        t = Table(title=f"Neo4j ({settings.neo4j_uri}) — UNREACHABLE", show_header=False)
        t.add_column("info")
        t.add_row(f"[red]{e}[/red]")
        return t

    try:
        with driver.session() as session:
            counts: dict[str, int] = {}
            labels = ["Processo", "Atividade", "Documento", "Unidade", "Orgao", "Usuario",
                      "TipoAcao", "GrupoAtividade", "PessoaFisica", "PessoaJuridica"]
            for lab in labels:
                rec = session.run(f"MATCH (n:{lab}) RETURN count(n) AS c").single()
                counts[lab] = int(rec["c"]) if rec else 0
            edges = ["DO_PROCESSO", "EXECUTADO_PELA_UNIDADE", "PASSOU_PELA_UNIDADE",
                     "PASSOU_PELO_ORGAO", "SEGUIDA_POR", "MENCIONA_PESSOA"]
            edge_counts: dict[str, int] = {}
            for e in edges:
                rec = session.run(f"MATCH ()-[r:{e}]->() RETURN count(r) AS c").single()
                edge_counts[e] = int(rec["c"]) if rec else 0
    finally:
        driver.close()

    t = Table(title=f"Neo4j ({settings.neo4j_uri})", show_header=False, box=None)
    t.add_column("entity", style="cyan")
    t.add_column("count", justify="right")
    for k, n in counts.items():
        t.add_row(f"  {k}", f"{n:,}")
    t.add_row("", "")
    for k, n in edge_counts.items():
        t.add_row(f"  edge:{k}", f"{n:,}")
    return t


def _checkpoints_table(state_dir: Path) -> Table:
    sdir = StateDir(state_dir)
    records = sdir.all_records()
    table = Table(title=f"Stage checkpoints ({state_dir})", show_lines=False)
    table.add_column("Stage", style="cyan")
    table.add_column("Status")
    table.add_column("When")
    table.add_column("Mode")
    table.add_column("Summary", overflow="fold")
    for s in all_stages():
        rec = records.get(s.meta.name)
        if rec is None:
            table.add_row(s.meta.name, "[dim]nunca rodou[/dim]", "—", "—", "—")
            continue
        if rec.is_complete:
            stat = "[green]✓ done[/green]"
            when = rec.completed_at or "?"
        elif rec.failed_at:
            stat = "[red]✗ failed[/red]"
            when = rec.failed_at
        else:
            stat = "[yellow]running[/yellow]"
            when = rec.started_at or "?"
        summary = ", ".join(f"{k}={v}" for k, v in (rec.summary or {}).items() if k != "traceback")
        table.add_row(s.meta.name, stat, when, rec.mode or "—", summary or "—")
    return table


def register(app: typer.Typer) -> None:
    @app.command("status")
    def status_cmd(
        state_dir: Optional[Path] = typer.Option(None, "--state-dir"),
        neo4j_uri: Optional[str] = typer.Option(None, "--neo4j-uri"),
        neo4j_user: Optional[str] = typer.Option(None, "--neo4j-user"),
        neo4j_password: Optional[str] = typer.Option(None, "--neo4j-password"),
    ) -> None:
        """Show pipeline state: filesystem counts, Neo4j metrics, checkpoint history."""
        settings = Settings.from_env()
        if neo4j_uri:
            settings = settings.overlay(neo4j_uri=neo4j_uri)
        if neo4j_user:
            settings = settings.overlay(neo4j_user=neo4j_user)
        if neo4j_password:
            settings = settings.overlay(neo4j_password=neo4j_password)

        sd = state_dir or Path("./.pipeline-state")
        console.print(Panel.fit("[bold]Pipeline status[/bold]", style="blue"))
        console.print(_filesystem_table())
        nt = _neo4j_table(settings)
        if nt is not None:
            console.print(nt)
        else:
            console.print("[dim]Neo4j: credentials not configured (set NEO4J_PASSWORD or pass --neo4j-password)[/dim]")
        console.print(_checkpoints_table(sd))
