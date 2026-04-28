"""Backward-compatible entry point for the ETL macro.

Today this is a thin shim around ``pipeline.runner.run_stages(["etl"], ...)``.
The "etl" stage in the registry is a virtual macro that hard-depends on
``precreate``, ``atividades``, ``timeline`` and ``permanencia``, so the
runner expands it into the full sequence.

Run via:
    python -m pipeline.etl --from 2025-01-01 --to 2026-01-01
    python -m pipeline.etl --dry-run
    python -m pipeline.etl --emit-json /tmp/emit
    python -m pipeline.etl --read-json /tmp/emit  # replay phases C/D

Going forward, the canonical CLI is::

    python -m pipeline run etl
    python -m pipeline run atividades         # only A+B
    python -m pipeline                          # interactive wizard
"""

from __future__ import annotations

import argparse
import logging

from . import runner
from . import stages  # noqa: F401  -- import-time side effect: populate registry
from .cli import add_standard_args, resolve_settings
from .logging_setup import configure_logging

log = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m pipeline.etl",
        description="ETL macro (precreate → atividades → timeline → permanencia).",
    )
    parser.add_argument("--from", dest="from_date", type=str, help="Filter from date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=str, help="Filter to date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Classify only, no Neo4j writes")
    parser.add_argument("--batch-size", type=int, default=500, help="Neo4j batch size (default: 500)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (default: 8)")
    parser.add_argument("--chunk-size", type=int, default=200, help="Processos per worker chunk (default: 200)")
    parser.add_argument("--skip-timeline", action="store_true", help="Skip building SEGUIDA_POR DAG")
    parser.add_argument("--skip-permanencia", action="store_true", help="Skip computing permanencia")
    parser.add_argument("--force", action="store_true", help="Re-run even if checkpoints exist")
    add_standard_args(parser, skip={"--batch-size", "--workers"})
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    settings = resolve_settings(args)
    configure_logging(__name__, settings.log_level)

    # The "etl" macro pulls in precreate + atividades + timeline + permanencia +
    # situacao. --skip-timeline / --skip-permanencia narrow it to subsets.
    # situacao always runs since the open/closed state is cheap and
    # downstream UI depends on it.
    stages_to_run: list[str] = ["precreate", "atividades"]
    if not args.skip_timeline:
        stages_to_run.append("timeline")
    if not args.skip_permanencia:
        stages_to_run.append("permanencia")
    stages_to_run.append("situacao")

    flags = {
        "from_date": args.from_date,
        "to_date": args.to_date,
        "workers": args.workers,
        "batch_size": args.batch_size,
        "chunk_size": args.chunk_size,
        "dry_run": args.dry_run,
    }

    runner.run_stages(
        stages_to_run,
        settings=settings,
        flags=flags,
        force=args.force,
    )


if __name__ == "__main__":
    main()
