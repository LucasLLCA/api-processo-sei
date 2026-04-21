"""Standard CLI arguments and settings resolution.

Scripts that want pipeline-standard flags call::

    parser = argparse.ArgumentParser(...)
    pipeline.cli.add_standard_args(parser)
    # ... script-specific args ...
    args = parser.parse_args()
    settings = pipeline.cli.resolve_settings(args)

Precedence: CLI flag > env var > .env > dataclass default.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from .config import Settings

_STANDARD_ARGS: tuple[tuple[str, dict], ...] = (
    ("--neo4j-uri", {"dest": "neo4j_uri", "default": None, "help": "Neo4j bolt URI"}),
    ("--neo4j-user", {"dest": "neo4j_user", "default": None, "help": "Neo4j username"}),
    ("--neo4j-password", {"dest": "neo4j_password", "default": None, "help": "Neo4j password"}),
    ("--neo4j-database", {"dest": "neo4j_database", "default": None, "help": "Neo4j database name"}),
    ("--batch-size", {"dest": "batch_size", "type": int, "default": None, "help": "Batch size for UNWIND operations"}),
    ("--workers", {"dest": "workers", "type": int, "default": None, "help": "Worker count for parallel stages"}),
    ("--log-level", {"dest": "log_level", "default": None, "help": "Logging level (DEBUG/INFO/WARNING/ERROR)"}),
    ("--emit-json", {"dest": "emit_json_dir", "type": Path, "default": None, "help": "Emit graph output as NDJSON under this directory"}),
    ("--read-json", {"dest": "read_json_dir", "type": Path, "default": None, "help": "Read graph input from this NDJSON emit directory"}),
)


def add_standard_args(parser: argparse.ArgumentParser, *, skip: set[str] | None = None) -> None:
    """Add the standard pipeline flags to `parser`.

    Pass `skip={"--batch-size", ...}` to omit flags the caller already declares
    with different defaults.
    """
    skip = skip or set()
    group = parser.add_argument_group("pipeline standard options")
    for flag, kwargs in _STANDARD_ARGS:
        if flag in skip:
            continue
        group.add_argument(flag, **kwargs)


def resolve_settings(args: argparse.Namespace) -> Settings:
    """Load settings from env/.env and overlay CLI values."""
    base = Settings.from_env()
    return base.overlay(
        neo4j_uri=getattr(args, "neo4j_uri", None),
        neo4j_user=getattr(args, "neo4j_user", None),
        neo4j_password=getattr(args, "neo4j_password", None),
        neo4j_database=getattr(args, "neo4j_database", None),
        batch_size=getattr(args, "batch_size", None),
        workers=getattr(args, "workers", None),
        log_level=getattr(args, "log_level", None),
        emit_json_dir=getattr(args, "emit_json_dir", None),
        read_json_dir=getattr(args, "read_json_dir", None),
    )
