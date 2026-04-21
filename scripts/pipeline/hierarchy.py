"""Unidade hierarchy helpers.

Preserves the exact behavior of the private helpers previously defined in
etl_neo4j.py (`_parent_unidade`, `_all_ancestor_unidades`).
"""

from __future__ import annotations


def parent_unidade(sigla: str) -> str | None:
    """Return the parent unidade sigla, or None for top-level units.

    'SEAD-PI/GAB/NTGD' -> 'SEAD-PI/GAB'
    'SEAD-PI'           -> None
    """
    parts = sigla.split("/")
    if len(parts) <= 1:
        return None
    return "/".join(parts[:-1])


def all_ancestor_unidades(sigla: str) -> list[str]:
    """Return all ancestor siglas, from immediate parent up to the root.

    'A/B/C' -> ['A/B', 'A']
    'A'     -> []
    """
    ancestors: list[str] = []
    current = sigla
    while True:
        parent = parent_unidade(current)
        if parent is None:
            break
        ancestors.append(parent)
        current = parent
    return ancestors
