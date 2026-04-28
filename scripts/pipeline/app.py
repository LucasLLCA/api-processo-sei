"""Typer root for the unified pipeline CLI.

Entry points:
    pipeline                 # interactive wizard (default)
    pipeline run STAGE...    # run with deps auto-resolved
    pipeline list            # list registered stages
    pipeline graph           # show DAG order
    pipeline status          # filesystem + Neo4j + checkpoints
"""

from __future__ import annotations

import typer

from . import stages  # noqa: F401  -- side effect: populate registry

app = typer.Typer(
    name="pipeline",
    help="SEAD-PI processo visualizer pipeline (Postgres → Neo4j + documentos + NER).",
    no_args_is_help=False,
    add_completion=False,
)

# Register subcommands. Each ``register(app)`` adds @app.command(...)s
# (and the interactive callback is set up before others to handle no-args).
from .apps import interactive, list as list_app, run as run_app, status as status_app  # noqa: E402

interactive.register(app)
run_app.register(app)
list_app.register(app)
status_app.register(app)


if __name__ == "__main__":
    app()
