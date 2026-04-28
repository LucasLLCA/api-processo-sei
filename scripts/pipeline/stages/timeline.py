"""Stage ``timeline`` — build the SEGUIDA_POR DAG (was Phase C).

Reads each processo's activity timeline from the configured
``GraphReader``, computes flow + independent edges via
``pipeline.timeline.build_edges_for_processo``, and writes
SEGUIDA_POR / PRECEDIDA_POR / SEGUIDO_INDEPENDENTEMENTE_POR /
INICIOU_PROCESSO via the configured ``GraphWriter``.

Modes: ``neo4j``, ``json-emit``, ``json-replay``.
"""

from __future__ import annotations

import logging

from ..cypher import LOAD_TIMELINE_CYPHER
from ..readers import GraphReader
from ..registry import stage
from ..timeline import build_edges_for_processo
from ..writers import GraphWriter
from .._stage_base import RunContext, StageMeta

log = logging.getLogger(__name__)


def build_timeline(reader: GraphReader, writer: GraphWriter) -> None:
    """Build SEGUIDA_POR/PRECEDIDA_POR DAG + INICIOU_PROCESSO."""
    log.info("Phase C: Building timeline DAG (unidade-context)...")

    total = reader.count_processos()
    log.info("  Processing %d processos...", total)

    batch_size = 500
    total_edges = 0
    total_inicio = 0
    processed = 0

    for batch in reader.iter_processo_batches(batch_size=batch_size):
        all_flow_edges: list[dict] = []
        all_independent_edges: list[dict] = []
        inicio_rows: list[dict] = []
        for processo in batch:
            pf = processo.protocolo_formatado
            activities = processo.activities
            if not activities:
                continue

            flow_edges, independent_edges = build_edges_for_processo(activities)
            all_flow_edges.extend(flow_edges)
            all_independent_edges.extend(independent_edges)

            inicio_rows.append({
                "protocolo_formatado": pf,
                "first_id": activities[0]["source_id"],
            })

        # LOAD_TIMELINE is composite (MERGE SEGUIDA_POR + MERGE PRECEDIDA_POR
        # in one statement) — stays as execute_template.
        if all_flow_edges:
            for i in range(0, len(all_flow_edges), 500):
                sub = all_flow_edges[i:i + 500]
                writer.execute_template(
                    "load_timeline", LOAD_TIMELINE_CYPHER,
                    {"edges": sub}, phase="C",
                )
            total_edges += len(all_flow_edges)

        if all_independent_edges:
            writer.write_edges(
                "SEGUIDO_INDEPENDENTEMENTE_POR",
                "Atividade", {"from_id": "source_id"},
                "Atividade", {"to_id": "source_id"},
                all_independent_edges,
                phase="C",
                props=["ref_id"],
            )
            total_edges += len(all_independent_edges)

        if inicio_rows:
            writer.write_edges(
                "INICIOU_PROCESSO",
                "Processo", {"protocolo_formatado": "protocolo_formatado"},
                "Atividade", {"first_id": "source_id"},
                inicio_rows,
                phase="C",
            )
            total_inicio += len(inicio_rows)

        processed += len(batch)
        if processed % 5000 == 0 or processed >= total:
            log.info(
                "  Progress: %d/%d processos, %d edges, %d inicio",
                min(processed, total), total, total_edges, total_inicio,
            )

    log.info(
        "Phase C complete: %d edges (SEGUIDA_POR + SEGUIDO_INDEPENDENTEMENTE_POR) + %d INICIOU_PROCESSO",
        total_edges, total_inicio,
    )


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------
@stage(StageMeta(
    name="timeline",
    description="Constrói SEGUIDA_POR/PRECEDIDA_POR via tracking de unidade.",
    type="core",
    depends_on=("atividades",),
    modes=("neo4j", "json-emit", "json-replay"),
    estimated_duration="~5-15min para 100k processos",
))
def run(ctx: RunContext) -> None:
    reader = ctx.require_reader()
    writer = ctx.require_writer()
    writer.open_phase("C")
    try:
        build_timeline(reader, writer)
    finally:
        writer.close_phase("C")
