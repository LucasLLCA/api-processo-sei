"""Stage ``situacao`` — derive open/closed state per (processo, unidade, orgao).

For each processo, walks its activities through ``compute_processo_state``
to derive cycles + unit/orgao states, then writes:

- ``SITUACAO_PROCESSO_UNIDADE`` (always, full state) and
  ``EM_ABERTO_NA_UNIDADE`` (sparse, only when open).
- ``SITUACAO_PROCESSO_ORGAO`` and ``EM_ABERTO_NO_ORGAO``.
- ``:Ciclo`` nodes with ``DO_PROCESSO`` / ``NA_UNIDADE`` / ``ABERTO_POR`` /
  ``FECHADO_POR`` edges.
- ``ULTIMA_ATIVIDADE_EM_ABERTO`` (one per open unit) and
  ``ATIVIDADE_MAIS_RECENTE`` (one per processo).
- Properties on ``:Processo``: ``situacao``, ``unidades_em_aberto_count``,
  ``data_inicio``, ``data_conclusao_global``, ``duracao_lifetime_horas``, …

Idempotency: before each batch, drops existing :Ciclo nodes and the
sparse edges (``EM_ABERTO_*``, ``ULTIMA_ATIVIDADE_EM_ABERTO``,
``ATIVIDADE_MAIS_RECENTE``) for the processos in the batch. Full-state
edges and node properties use MERGE+SET (idempotent).

Modes: ``neo4j``, ``json-emit``, ``json-replay``.
Hard-depends on ``atividades``. Soft-depends on ``permanencia`` (when both
run together they share ``ProcessoState`` via ``ctx.cached``).
"""

from __future__ import annotations

import logging

from ..cypher import (
    CLEAR_SITUACAO_FOR_PROTOCOLOS_CYPHER,
    LOAD_ATIVIDADE_MAIS_RECENTE_CYPHER,
    LOAD_CICLOS_CYPHER,
    LOAD_CICLOS_FECHADO_POR_CYPHER,
    LOAD_EM_ABERTO_ORGAO_CYPHER,
    LOAD_EM_ABERTO_UNIDADE_CYPHER,
    LOAD_PROCESSO_SITUACAO_PROPS_CYPHER,
    LOAD_SITUACAO_ORGAO_CYPHER,
    LOAD_SITUACAO_UNIDADE_CYPHER,
    LOAD_ULTIMA_ATIVIDADE_EM_ABERTO_CYPHER,
)
from ..process_state import compute_processo_state
from ..readers import GraphReader
from ..registry import stage
from ..writers import GraphWriter
from .._stage_base import RunContext, StageMeta

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Core: process one batch end-to-end
# ---------------------------------------------------------------------------
def _process_batch(batch, writer: GraphWriter, ctx: RunContext | None) -> dict[str, int]:
    """Compute state for each processo in batch and emit all rows.

    Returns a counter dict for telemetry.
    """
    protocolos: list[str] = []
    sit_unidade_rows: list[dict] = []
    sit_orgao_rows: list[dict] = []
    em_aberto_uni_rows: list[dict] = []
    em_aberto_orgao_rows: list[dict] = []
    ciclo_rows: list[dict] = []
    fechado_por_rows: list[dict] = []
    ultima_aberto_rows: list[dict] = []
    mais_recente_rows: list[dict] = []
    processo_props_rows: list[dict] = []

    # Map source_id → (unidade) so we can resolve ULTIMA_ATIVIDADE_EM_ABERTO
    # without re-querying the graph (we already have it from the reader).
    for processo in batch:
        pf = processo.protocolo_formatado
        timeline = processo.activities
        if not timeline:
            continue
        protocolos.append(pf)

        # Reuse cached state when permanencia ran first in this same run.
        if ctx is not None:
            state = ctx.cached(
                f"process_state:{pf}",
                lambda t=timeline, p=pf: compute_processo_state(t, protocolo_formatado=p),
            )
        else:
            state = compute_processo_state(timeline, protocolo_formatado=pf)

        proc = state["processo"]
        processo_props_rows.append({
            "protocolo_formatado": pf,
            "situacao": proc["situacao"],
            "unidades_em_aberto_count": proc["unidades_em_aberto_count"],
            "unidades_em_aberto_siglas": proc["unidades_em_aberto"],
            "data_inicio": proc["data_inicio"],
            "data_ultima_atividade": proc["data_ultima_atividade"],
            "data_conclusao_global": proc["data_conclusao_global"],
            "duracao_lifetime_horas": proc["duracao_lifetime_horas"],
            "duracao_lifetime_dias": proc["duracao_lifetime_dias"],
            "situacao_computed_at": proc["situacao_computed_at"],
        })

        # Atividade mais recente (always, every processo)
        if proc["atividade_mais_recente_id"] is not None:
            mais_recente_rows.append({
                "protocolo_formatado": pf,
                "atividade_id": proc["atividade_mais_recente_id"],
            })

        # Per-unidade rows
        for u in state["unidades"]:
            sit_unidade_rows.append({
                "protocolo_formatado": pf,
                "unidade": u["unidade"],
                "situacao": u["situacao"],
                "duracao_acumulada_horas": u["duracao_acumulada_horas"],
                "duracao_lifetime_horas": u["duracao_lifetime_horas"],
                "visitas": u["visitas"],
                "primeira_entrada": u["primeira_entrada"],
                "ultima_saida": u["ultima_saida"],
                "ultima_atividade_id": u["ultima_atividade_id"],
                "ultima_atividade_data_hora": u["ultima_atividade_data_hora"],
                "ultima_atividade_tipo_acao": u["ultima_atividade_tipo_acao"],
                "dias_sem_atividade": u["dias_sem_atividade"],
            })

            if u["situacao"] == "em_aberto":
                em_aberto_uni_rows.append({
                    "protocolo_formatado": pf,
                    "unidade": u["unidade"],
                    "desde": u["primeira_entrada"],
                    "dias_sem_atividade": u["dias_sem_atividade"],
                })
                # ULTIMA_ATIVIDADE_EM_ABERTO: one per open unit
                if u["ultima_atividade_id"] is not None:
                    ultima_aberto_rows.append({
                        "protocolo_formatado": pf,
                        "atividade_id": u["ultima_atividade_id"],
                    })

            # Cycles → :Ciclo nodes
            for c in u["ciclos"]:
                ciclo_rows.append({
                    "protocolo_formatado": pf,
                    "unidade": u["unidade"],
                    "id": c["id"],
                    "ordem": c["ordem"],
                    "entrada": c["entrada"],
                    "saida": c["saida"],
                    "duracao_horas": c["duracao_horas"],
                    "status": c["status"],
                    "implicit_close": bool(c.get("implicit_close", False)),
                    "abertura_atividade_id": c["abertura_atividade_id"],
                })
                if c["status"] == "concluida" and c["conclusao_atividade_id"] is not None:
                    fechado_por_rows.append({
                        "id": c["id"],
                        "conclusao_atividade_id": c["conclusao_atividade_id"],
                    })

        # Per-orgao rows
        for o in state["orgaos"]:
            sit_orgao_rows.append({
                "protocolo_formatado": pf,
                "orgao": o["orgao"],
                "situacao": o["situacao"],
                "unidades_abertas_count": o["unidades_abertas_count"],
                "duracao_acumulada_horas": o["duracao_acumulada_horas"],
                "duracao_lifetime_horas": o["duracao_lifetime_horas"],
                "dias_sem_atividade": o["dias_sem_atividade"],
            })
            if o["situacao"] == "em_aberto":
                em_aberto_orgao_rows.append({
                    "protocolo_formatado": pf,
                    "orgao": o["orgao"],
                    "desde": o["primeira_entrada"],
                    "unidades_abertas_count": o["unidades_abertas_count"],
                })

    # ── Idempotency: clear sparse edges + cycles for this batch ──
    if protocolos:
        writer.execute_template(
            "clear_situacao", CLEAR_SITUACAO_FOR_PROTOCOLOS_CYPHER,
            {"protocolos": protocolos}, phase="situacao",
        )

    # ── Emit all rows ──
    def _flush(name, cypher, rows, chunk=500):
        if not rows:
            return
        for i in range(0, len(rows), chunk):
            writer.execute_template(
                name, cypher, {"rows": rows[i:i + chunk]}, phase="situacao",
            )

    _flush("load_ciclos", LOAD_CICLOS_CYPHER, ciclo_rows)
    _flush("load_ciclos_fechado_por", LOAD_CICLOS_FECHADO_POR_CYPHER, fechado_por_rows)
    _flush("load_situacao_unidade", LOAD_SITUACAO_UNIDADE_CYPHER, sit_unidade_rows)
    _flush("load_em_aberto_unidade", LOAD_EM_ABERTO_UNIDADE_CYPHER, em_aberto_uni_rows)
    _flush("load_situacao_orgao", LOAD_SITUACAO_ORGAO_CYPHER, sit_orgao_rows)
    _flush("load_em_aberto_orgao", LOAD_EM_ABERTO_ORGAO_CYPHER, em_aberto_orgao_rows)
    _flush("load_ultima_atividade_em_aberto", LOAD_ULTIMA_ATIVIDADE_EM_ABERTO_CYPHER, ultima_aberto_rows)
    _flush("load_atividade_mais_recente", LOAD_ATIVIDADE_MAIS_RECENTE_CYPHER, mais_recente_rows)
    _flush("load_processo_situacao_props", LOAD_PROCESSO_SITUACAO_PROPS_CYPHER, processo_props_rows)

    return {
        "processos": len(protocolos),
        "ciclos": len(ciclo_rows),
        "ciclos_concluidos": len(fechado_por_rows),
        "situacao_unidade": len(sit_unidade_rows),
        "em_aberto_unidade": len(em_aberto_uni_rows),
        "situacao_orgao": len(sit_orgao_rows),
        "em_aberto_orgao": len(em_aberto_orgao_rows),
        "ultima_atividade_em_aberto": len(ultima_aberto_rows),
        "atividade_mais_recente": len(mais_recente_rows),
    }


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------
@stage(StageMeta(
    name="situacao",
    description="Computa situação (em aberto / concluído) por unidade, órgão e processo + ciclos.",
    type="core",
    depends_on=("atividades",),
    soft_depends_on=("permanencia",),
    modes=("neo4j", "json-emit", "json-replay"),
    estimated_duration="~5-15min para 100k processos",
))
def run(ctx: RunContext) -> None:
    reader = ctx.require_reader()
    writer = ctx.require_writer()
    writer.open_phase("situacao")
    try:
        total = reader.count_processos()
        log.info("situacao: computing for %d processos...", total)

        totals: dict[str, int] = {}
        processed = 0
        batch_size = 500

        for batch in reader.iter_processo_batches(batch_size=batch_size):
            counts = _process_batch(batch, writer, ctx)
            for k, v in counts.items():
                totals[k] = totals.get(k, 0) + v
            processed += len(batch)
            if processed % 5000 == 0 or processed >= total:
                log.info(
                    "  Progress: %d/%d processos | ciclos=%d | em_aberto_unidade=%d | em_andamento=%d",
                    min(processed, total), total,
                    totals.get("ciclos", 0),
                    totals.get("em_aberto_unidade", 0),
                    totals.get("em_aberto_orgao", 0),
                )

        log.info(
            "situacao complete: ciclos=%d (%d concluidos), "
            "SITUACAO_PROCESSO_UNIDADE=%d, EM_ABERTO_NA_UNIDADE=%d, "
            "ATIVIDADE_MAIS_RECENTE=%d, ULTIMA_ATIVIDADE_EM_ABERTO=%d",
            totals.get("ciclos", 0), totals.get("ciclos_concluidos", 0),
            totals.get("situacao_unidade", 0), totals.get("em_aberto_unidade", 0),
            totals.get("atividade_mais_recente", 0), totals.get("ultima_atividade_em_aberto", 0),
        )
        ctx.cache["situacao_summary"] = totals
    finally:
        writer.close_phase("situacao")
