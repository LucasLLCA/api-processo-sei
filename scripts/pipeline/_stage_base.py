"""Stage protocol primitives.

Every stage module declares ``META: StageMeta`` and a ``run(ctx: RunContext)``
function (or uses the ``@stage`` decorator from ``pipeline.registry``).
The runner builds the ``RunContext`` and dispatches in topological order.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal

from .config import Settings


# ---------------------------------------------------------------------------
# I/O modes a stage may declare support for
# ---------------------------------------------------------------------------
# - "neo4j"        : write directly to a live Neo4j driver
# - "json-emit"    : write to NDJSON via JsonFileWriter (no Neo4j needed)
# - "json-replay"  : read from a JsonFileReader emit-dir; usually paired with
#                    "neo4j" or "json-emit" on the write side
# - "fs"           : pure filesystem in/out, no graph involved
# - "postgres"     : reads/writes Postgres only
Mode = Literal["neo4j", "json-emit", "json-replay", "fs", "postgres"]

StageType = Literal["bootstrap", "core", "enrich", "op"]


@dataclass(frozen=True)
class StageMeta:
    """Declarative metadata for a stage.

    ``name`` is the user-visible stage identifier (kebab-case is fine).
    ``depends_on`` is resolved transitively by the runner before execution.
    ``soft_depends_on`` only generates a warning when missing.
    ``modes`` is the subset of ``Mode`` values this stage knows how to run in.
    """

    name: str
    description: str
    type: StageType
    depends_on: tuple[str, ...] = ()
    soft_depends_on: tuple[str, ...] = ()
    modes: tuple[Mode, ...] = ()
    can_skip_when_done: bool = True
    estimated_duration: str = ""


# ---------------------------------------------------------------------------
# Execution context shared across stages within a single run
# ---------------------------------------------------------------------------
@dataclass
class RunContext:
    settings: Settings
    mode: Mode
    flags: dict[str, Any]
    state_dir: Path
    cache: dict[str, Any] = field(default_factory=dict)
    writer: Any | None = None       # GraphWriter
    reader: Any | None = None       # GraphReader
    driver: Any | None = None       # raw neo4j.Driver

    def require_writer(self) -> Any:
        if self.writer is None:
            raise RuntimeError(
                f"Stage requires a GraphWriter but mode={self.mode!r} did not "
                f"build one. Set --mode neo4j or --mode json-emit."
            )
        return self.writer

    def require_reader(self) -> Any:
        if self.reader is None:
            raise RuntimeError(
                f"Stage requires a GraphReader but mode={self.mode!r} did not "
                f"build one. Set --mode neo4j or --mode json-replay."
            )
        return self.reader

    def require_driver(self) -> Any:
        if self.driver is None:
            raise RuntimeError(
                "Stage requires a raw Neo4j driver. Use --mode neo4j."
            )
        return self.driver

    def cached(self, key: str, factory: Callable[[], Any]) -> Any:
        """Lazy-compute and memoize a value across stages in the same run.

        Stages that need shared discovery output (e.g. processo list) call
        ``ctx.cached('discovery', lambda: ...)`` and the factory runs at most
        once per pipeline invocation.
        """
        if key not in self.cache:
            self.cache[key] = factory()
        return self.cache[key]


# Type alias for stage runner functions.
StageFunc = Callable[[RunContext], None]
