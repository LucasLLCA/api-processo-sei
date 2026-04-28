"""Pure-Python permanência aggregation.

Thin shim around ``pipeline.process_state.compute_processo_state``: re-exposes
``compute_permanencia_for_processo`` for back-compat callers, but the cycle
algorithm lives in process_state. Permanência stage and situação stage share
the same compute via ``ctx.cached``.

Output shape is preserved (``(unidade_records, orgao_records)`` tuple) but
each record now carries ``duracao_acumulada_horas`` (sum of closed cycles)
and ``duracao_lifetime_horas`` (first_entrada → last_significant_activity)
in addition to the legacy ``duracao_total_horas`` alias.
"""

from __future__ import annotations

from typing import Any

from .process_state import compute_processo_state


def compute_permanencia_for_processo(
    timeline: list[dict[str, Any]],
    *,
    protocolo_formatado: str | None = None,
    now_iso: str | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (unidade_records, orgao_records) for one processo.

    Records carry both the new fields (``duracao_acumulada_horas``,
    ``duracao_lifetime_horas``, ``visitas``, ``primeira_entrada``,
    ``ultima_saida``) and the legacy ``duracao_total_horas`` alias so that
    existing readers don't need an immediate breaking change.
    """
    state = compute_processo_state(
        timeline,
        protocolo_formatado=protocolo_formatado,
        now_iso=now_iso,
    )

    unidade_records = [
        {
            "unidade": u["unidade"],
            "duracao_total_horas": u["duracao_acumulada_horas"],     # back-compat alias
            "duracao_acumulada_horas": u["duracao_acumulada_horas"],
            "duracao_lifetime_horas": u["duracao_lifetime_horas"],
            "visitas": u["visitas"],
            "primeira_entrada": u["primeira_entrada"],
            "ultima_saida": u["ultima_saida"] or u["primeira_entrada"],
        }
        for u in state["unidades"]
    ]
    orgao_records = [
        {
            "orgao": o["orgao"],
            "duracao_total_horas": o["duracao_acumulada_horas"],     # back-compat alias
            "duracao_acumulada_horas": o["duracao_acumulada_horas"],
            "duracao_lifetime_horas": o["duracao_lifetime_horas"],
            "visitas": o["visitas"],
            "primeira_entrada": o["primeira_entrada"],
            "ultima_saida": o["ultima_saida"] or o["primeira_entrada"],
        }
        for o in state["orgaos"]
    ]
    return unidade_records, orgao_records
