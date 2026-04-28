"""Stage ``permanencia`` — PASSOU_PELA_UNIDADE / PASSOU_PELO_ORGAO (was Phase D).

Reads each processo's activity timeline, aggregates stints via
``pipeline.permanencia.compute_permanencia_for_processo``, and writes the
results through the configured ``GraphWriter``.

Modes: ``neo4j``, ``json-emit``, ``json-replay``.
"""

from __future__ import annotations

import logging

from ..cypher import LOAD_PERMANENCIA_ORGAO_CYPHER, LOAD_PERMANENCIA_UNIDADE_CYPHER
from ..process_state import compute_processo_state
from ..readers import GraphReader
from ..registry import stage
from ..writers import GraphWriter
from .._stage_base import RunContext, StageMeta

log = logging.getLogger(__name__)


def compute_permanencia(reader: GraphReader, writer: GraphWriter, ctx=None) -> None:
    """Compute time each processo spent per unidade and per orgao.

    Uses ``compute_processo_state`` (cycle-aware) and writes the new
    duracao_acumulada / duracao_lifetime properties alongside the legacy
    duracao_total alias on PASSOU_PELA_UNIDADE/PASSOU_PELO_ORGAO. When
    ``ctx`` is given, results are stashed in ``ctx.cache`` so the
    ``situacao`` stage reuses them.
    """
    log.info("permanencia: computing PASSOU_PELA_UNIDADE + PASSOU_PELO_ORGAO...")

    total = reader.count_processos()
    log.info("  Computing for %d processos...", total)

    batch_size = 1000
    total_unidade_links = 0
    total_orgao_links = 0
    processed = 0

    for batch in reader.iter_processo_batches(batch_size=batch_size):
        unidade_rows: list[dict] = []
        orgao_rows: list[dict] = []
        for processo in batch:
            pf = processo.protocolo_formatado
            timeline = processo.activities
            if not timeline:
                continue

            # Cache by protocolo so the situacao stage can reuse this exact
            # ProcessoState without re-walking activities.
            if ctx is not None:
                state = ctx.cached(
                    f"process_state:{pf}",
                    lambda t=timeline, p=pf: compute_processo_state(t, protocolo_formatado=p),
                )
            else:
                state = compute_processo_state(timeline, protocolo_formatado=pf)

            for u in state["unidades"]:
                unidade_rows.append({
                    "protocolo_formatado": pf,
                    "unidade": u["unidade"],
                    "duracao_total_horas": u["duracao_acumulada_horas"],  # back-compat
                    "duracao_acumulada_horas": u["duracao_acumulada_horas"],
                    "duracao_lifetime_horas": u["duracao_lifetime_horas"],
                    "visitas": u["visitas"],
                    "primeira_entrada": u["primeira_entrada"],
                    "ultima_saida": u["ultima_saida"] or u["primeira_entrada"],
                })
            for o in state["orgaos"]:
                orgao_rows.append({
                    "protocolo_formatado": pf,
                    "orgao": o["orgao"],
                    "duracao_total_horas": o["duracao_acumulada_horas"],
                    "duracao_acumulada_horas": o["duracao_acumulada_horas"],
                    "duracao_lifetime_horas": o["duracao_lifetime_horas"],
                    "visitas": o["visitas"],
                    "primeira_entrada": o["primeira_entrada"],
                    "ultima_saida": o["ultima_saida"] or o["primeira_entrada"],
                })

        # PASSOU_PELA_UNIDADE/ORGAO carry datetime() function calls in their
        # SET clauses, so they stay as execute_template.
        if unidade_rows:
            for i in range(0, len(unidade_rows), 500):
                sub = unidade_rows[i:i + 500]
                writer.execute_template(
                    "load_permanencia_unidade", LOAD_PERMANENCIA_UNIDADE_CYPHER,
                    {"rows": sub}, phase="D",
                )
            total_unidade_links += len(unidade_rows)

        if orgao_rows:
            for i in range(0, len(orgao_rows), 500):
                sub = orgao_rows[i:i + 500]
                writer.execute_template(
                    "load_permanencia_orgao", LOAD_PERMANENCIA_ORGAO_CYPHER,
                    {"rows": sub}, phase="D",
                )
            total_orgao_links += len(orgao_rows)

        processed += len(batch)
        if processed % 5000 == 0 or processed >= total:
            log.info(
                "  Progress: %d/%d processos, %d PASSOU_PELA_UNIDADE, %d PASSOU_PELO_ORGAO",
                min(processed, total), total, total_unidade_links, total_orgao_links,
            )

    log.info(
        "Phase D complete: %d PASSOU_PELA_UNIDADE + %d PASSOU_PELO_ORGAO",
        total_unidade_links, total_orgao_links,
    )


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------
@stage(StageMeta(
    name="permanencia",
    description="Computa permanência (ciclos entrada→conclusão) por unidade e órgão.",
    type="core",
    depends_on=("atividades",),
    modes=("neo4j", "json-emit", "json-replay"),
    estimated_duration="~5-15min para 100k processos",
))
def run(ctx: RunContext) -> None:
    reader = ctx.require_reader()
    writer = ctx.require_writer()
    writer.open_phase("D")
    try:
        compute_permanencia(reader, writer, ctx=ctx)
    finally:
        writer.close_phase("D")
