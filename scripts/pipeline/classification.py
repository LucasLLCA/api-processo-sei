"""Canonical home for text-based classification helpers.

`extract_orgao` used to be reimplemented in etl_neo4j_classifier.py and again
(inline) in etl_neo4j.py. This is the single source of truth.
"""

from __future__ import annotations


def extract_orgao(unidade: str | None) -> str:
    """Extract the orgao from a unidade sigla.

    'SEAD-PI/GAB/SGACG' -> 'SEAD-PI'
    None / empty -> 'DESCONHECIDO'
    """
    if not unidade:
        return "DESCONHECIDO"
    return unidade.split("/")[0]
