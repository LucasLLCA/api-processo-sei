"""Pipeline execution memory: per-stage checkpoints + run log.

Layout (default ``./.pipeline-state``)::

    .pipeline-state/
        runs.jsonl         # append-only log: each run as one JSON line
        stages/
            <name>.json    # last completion record per stage
        lock                # PID lockfile (optional, advisory)
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------
@dataclass
class StageRecord:
    stage: str
    completed_at: str | None = None
    failed_at: str | None = None
    started_at: str | None = None
    mode: str | None = None
    args: dict[str, Any] = field(default_factory=dict)
    summary: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    @property
    def is_complete(self) -> bool:
        return self.completed_at is not None and self.failed_at is None


# ---------------------------------------------------------------------------
# State directory
# ---------------------------------------------------------------------------
class StateDir:
    """Read/write helper for the pipeline state directory."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.stages_path = self.path / "stages"
        self.runs_log = self.path / "runs.jsonl"
        self.lock_file = self.path / "lock"

    def ensure(self) -> None:
        self.stages_path.mkdir(parents=True, exist_ok=True)

    # ── Per-stage records ──
    def stage_path(self, stage: str) -> Path:
        return self.stages_path / f"{stage}.json"

    def read_stage(self, stage: str) -> StageRecord | None:
        p = self.stage_path(stage)
        if not p.is_file():
            return None
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        return StageRecord(**data)

    def write_stage(self, record: StageRecord) -> None:
        self.ensure()
        p = self.stage_path(record.stage)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(asdict(record), indent=2, default=str), encoding="utf-8")
        tmp.replace(p)

    def clear_stage(self, stage: str) -> bool:
        p = self.stage_path(stage)
        if p.is_file():
            p.unlink()
            return True
        return False

    def clear_all(self) -> int:
        if not self.stages_path.is_dir():
            return 0
        n = 0
        for f in self.stages_path.glob("*.json"):
            f.unlink()
            n += 1
        return n

    def all_records(self) -> dict[str, StageRecord]:
        if not self.stages_path.is_dir():
            return {}
        out: dict[str, StageRecord] = {}
        for f in sorted(self.stages_path.glob("*.json")):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                rec = StageRecord(**data)
                out[rec.stage] = rec
            except (json.JSONDecodeError, TypeError):
                continue
        return out

    # ── Run log (append-only) ──
    def append_run(self, payload: dict[str, Any]) -> None:
        self.ensure()
        with self.runs_log.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")

    def last_runs(self, n: int = 10) -> list[dict[str, Any]]:
        if not self.runs_log.is_file():
            return []
        lines = self.runs_log.read_text(encoding="utf-8").splitlines()
        out: list[dict[str, Any]] = []
        for line in lines[-n:]:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return out

    # ── Lock (advisory) ──
    def try_lock(self) -> bool:
        """Best-effort PID lock. Returns False if another fresh PID holds it."""
        self.ensure()
        if self.lock_file.is_file():
            try:
                pid = int(self.lock_file.read_text().strip())
                # If the PID is alive, refuse the lock
                try:
                    os.kill(pid, 0)
                    return False
                except (OSError, ProcessLookupError):
                    pass  # stale lock — overwrite
            except (ValueError, OSError):
                pass
        self.lock_file.write_text(str(os.getpid()), encoding="utf-8")
        return True

    def release_lock(self) -> None:
        if self.lock_file.is_file():
            try:
                self.lock_file.unlink()
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def args_compatible(a: dict, b: dict, *, fields: tuple[str, ...] | None = None) -> bool:
    """Compare relevant arg subsets to decide if a checkpoint is reusable.

    By default only compares fields explicitly named in ``fields``; when None
    we compare a conservative subset that affects output: from/to dates and
    the limit. CLI tweaks like --workers don't invalidate a checkpoint.
    """
    if fields is None:
        fields = ("from_date", "to_date", "limit", "from", "to")
    return {k: a.get(k) for k in fields} == {k: b.get(k) for k in fields}
