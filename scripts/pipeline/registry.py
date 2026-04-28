"""Global stage registry + dependency resolver.

Each stage module is registered via the ``@stage(meta)`` decorator. The
runner queries ``resolve(names)`` to get a topologically-sorted list of
``StageDef`` instances (with hard dependencies pulled in automatically).
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Iterable

from ._stage_base import Mode, StageFunc, StageMeta


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
@dataclass
class StageDef:
    meta: StageMeta
    func: StageFunc | None              # None when lazy_module is set
    lazy_module: str | None = None       # e.g. "bootstrap.populate_unidades_sei"
    lazy_func: str | None = None         # function name in that module

    def call(self, ctx) -> None:
        if self.func is not None:
            self.func(ctx)
            return
        # Lazy-import path: defer until invocation so api.* doesn't pollute
        # the pipeline package's import graph.
        assert self.lazy_module and self.lazy_func, "lazy_module/lazy_func required"
        module = importlib.import_module(self.lazy_module)
        getattr(module, self.lazy_func)(ctx)


_REGISTRY: dict[str, StageDef] = {}


def stage(meta: StageMeta):
    """Decorator: register a stage function.

    Usage::

        from pipeline.registry import stage
        from pipeline._stage_base import StageMeta

        @stage(StageMeta(name="atividades", ...))
        def run(ctx):
            ...
    """
    def decorator(func: StageFunc) -> StageFunc:
        if meta.name in _REGISTRY:
            existing = _REGISTRY[meta.name]
            raise RuntimeError(
                f"Stage {meta.name!r} already registered "
                f"(by {existing.func or existing.lazy_module})"
            )
        _REGISTRY[meta.name] = StageDef(meta=meta, func=func)
        return func
    return decorator


def register_lazy(meta: StageMeta, module: str, func: str) -> None:
    """Register a stage whose runner is in a separate module loaded on demand.

    Used by bootstrap stages that depend on ``api.*`` — keeping them lazy means
    the pipeline package can be imported in minimal environments.
    """
    if meta.name in _REGISTRY:
        existing = _REGISTRY[meta.name]
        raise RuntimeError(f"Stage {meta.name!r} already registered (by {existing})")
    _REGISTRY[meta.name] = StageDef(meta=meta, func=None, lazy_module=module, lazy_func=func)


def get(name: str) -> StageDef | None:
    return _REGISTRY.get(name)


def all_stages() -> list[StageDef]:
    return list(_REGISTRY.values())


def names() -> list[str]:
    return list(_REGISTRY.keys())


def clear() -> None:
    """Test helper: drop all registered stages."""
    _REGISTRY.clear()


# ---------------------------------------------------------------------------
# DAG resolver
# ---------------------------------------------------------------------------
class StageNotFoundError(KeyError):
    pass


class CyclicDependencyError(RuntimeError):
    pass


class IncompatibleModeError(ValueError):
    pass


def resolve(names: Iterable[str]) -> list[StageDef]:
    """Return ``StageDef``s for ``names`` plus their hard deps, in execution order.

    Each transitive ``depends_on`` entry is included. Cycles raise
    ``CyclicDependencyError``. Missing names raise ``StageNotFoundError``.
    """
    requested = list(names)
    for n in requested:
        if n not in _REGISTRY:
            raise StageNotFoundError(n)

    # DFS with three colors for cycle detection.
    WHITE, GRAY, BLACK = 0, 1, 2
    color: dict[str, int] = {}
    order: list[str] = []

    def visit(name: str) -> None:
        c = color.get(name, WHITE)
        if c == BLACK:
            return
        if c == GRAY:
            raise CyclicDependencyError(f"Cycle detected at {name!r}")
        color[name] = GRAY
        sd = _REGISTRY.get(name)
        if sd is None:
            raise StageNotFoundError(name)
        for dep in sd.meta.depends_on:
            visit(dep)
        color[name] = BLACK
        order.append(name)

    for n in requested:
        visit(n)

    return [_REGISTRY[n] for n in order]


def check_mode(stages: Iterable[StageDef], mode: Mode) -> list[str]:
    """Return a list of stage names that don't declare ``mode`` as supported.

    The runner uses this to error out early before any work happens.
    """
    bad: list[str] = []
    for s in stages:
        if mode not in s.meta.modes:
            bad.append(s.meta.name)
    return bad


def soft_dep_warnings(stages: Iterable[StageDef]) -> list[tuple[str, str]]:
    """Return (stage, missing_soft_dep) pairs for warnings."""
    selected_names = {s.meta.name for s in stages}
    warnings: list[tuple[str, str]] = []
    for s in stages:
        for soft in s.meta.soft_depends_on:
            if soft not in selected_names:
                warnings.append((s.meta.name, soft))
    return warnings
