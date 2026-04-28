"""Stage ``atividades`` — load atividades + per-atividade edges (was Phase B).

Each worker fetches a chunk of protocolos from PostgreSQL, classifies the
descriptions via ``pipeline.classifier.transform_row``, and writes through
the configured ``GraphWriter`` (Neo4j or NDJSON). The thread pool is
managed by the stage's ``run`` function below.

Modes: ``neo4j``, ``json-emit``. Hard-depends on ``precreate``.
"""

from __future__ import annotations

import logging
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2.extras

from ..classifier import transform_row
from ..config import Settings
from ..cypher import LOAD_ATIVIDADES_CYPHER, LOAD_DOCUMENTO_CYPHER
from ..discovery import discover
from ..postgres import make_pg_conn
from ..registry import stage
from ..sql import COUNT_ATIVIDADES_FOR_IDS_SQL, FETCH_ANDAMENTOS_SQL
from ..writers import GraphWriter
from .._stage_base import RunContext, StageMeta

log = logging.getLogger(__name__)


def load_atividades_batch(writer: GraphWriter, transformed: list[dict]) -> None:
    """Write a single batch of transformed atividades.

    Composite Cypher: 4× MATCH + MERGE Atividade + 4× MERGE edge in one
    statement. Optional REMETIDO_PELA_UNIDADE and REFERENCIA_DOCUMENTO
    edges are flushed alongside.
    """
    writer.execute_template(
        "load_atividades", LOAD_ATIVIDADES_CYPHER,
        {"rows": transformed}, phase="B",
    )

    tramitacao_rows = [
        {"source_id": t["source_id"], "source_unidade": t["source_unidade"]}
        for t in transformed
        if t["source_unidade"]
    ]
    if tramitacao_rows:
        writer.write_edges(
            "REMETIDO_PELA_UNIDADE",
            "Atividade", {"source_id": "source_id"},
            "Unidade", {"source_unidade": "sigla"},
            tramitacao_rows,
            phase="B",
        )

    documento_rows = [
        {
            "source_id": t["source_id"],
            "numero": t["doc_info"]["numero"],
            "tipo": t["doc_info"]["tipo"],
            "serie_id": t["doc_info"]["serie_id"],
        }
        for t in transformed
        if t["doc_info"]
    ]
    if documento_rows:
        writer.execute_template(
            "load_documento", LOAD_DOCUMENTO_CYPHER,
            {"rows": documento_rows}, phase="B",
        )


def process_chunk(
    protocolo_ids: list[str],
    writer: GraphWriter | None,
    dry_run: bool,
    batch_size: int,
    settings: Settings,
) -> tuple[int, Counter, list[str], set[str]]:
    """Worker: fetch andamentos for a chunk, classify, load atividades.

    Returns (rows_processed, classification_counter, sample_unclassified,
    discovered_unidades). Postgres connection is opened inside the worker
    so each thread gets its own connection.
    """
    pg_conn = make_pg_conn(settings)
    total_rows = 0
    stats: Counter = Counter()
    unclassified: list[str] = []
    unidades: set[str] = set()
    seq_counters: dict[str, int] = {}

    try:
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(COUNT_ATIVIDADES_FOR_IDS_SQL, (protocolo_ids,))
        debug_count = cursor.fetchone()[0]
        log.info(
            "  Chunk debug: %d IDs, %d matching rows, first IDs: %s, types: %s",
            len(protocolo_ids), debug_count, protocolo_ids[:3],
            [type(x).__name__ for x in protocolo_ids[:3]],
        )
        if debug_count == 0:
            log.warning(
                "  Chunk has 0 matching rows for %d IDs! Sample IDs: %s",
                len(protocolo_ids), protocolo_ids[:5],
            )

        cursor.execute(FETCH_ANDAMENTOS_SQL, (protocolo_ids,))

        batch: list[dict] = []
        for row in cursor:
            d = dict(row)
            pf = d["protocolo_formatado"]
            seq_counters[pf] = seq_counters.get(pf, 0) + 1
            t = transform_row(d, seq_counters[pf])
            batch.append(t)
            stats[t["tipo_acao"]] += 1
            unidades.add(t["unidade"])
            if t["source_unidade"]:
                unidades.add(t["source_unidade"])
            if t["tipo_acao"] == "OUTROS" and len(unclassified) < 20:
                desc = t["descricao"][:100]
                if desc not in unclassified:
                    unclassified.append(desc)

            if len(batch) >= batch_size:
                total_rows += len(batch)
                if not dry_run and writer is not None:
                    load_atividades_batch(writer, batch)
                batch = []

        if batch:
            total_rows += len(batch)
            if not dry_run and writer is not None:
                load_atividades_batch(writer, batch)

        cursor.close()
    finally:
        pg_conn.close()

    return total_rows, stats, unclassified, unidades


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------
@stage(StageMeta(
    name="atividades",
    description="Carrega Atividades + arestas (DO_PROCESSO, EXECUTADO_PELA_UNIDADE, …).",
    type="core",
    depends_on=("precreate",),
    soft_depends_on=("unidades",),
    modes=("neo4j", "json-emit"),
    estimated_duration="~20-60min para 1M atividades",
))
def run(ctx: RunContext) -> None:
    """Stage runner: parallel chunk processing of atividades."""
    ctx.settings.require_postgres()
    discovery = ctx.cached(
        "discovery",
        lambda: discover(
            ctx.settings,
            ctx.flags.get("from_date"),
            ctx.flags.get("to_date"),
        ),
    )
    protocolo_ids = discovery["protocolo_ids"]
    if not protocolo_ids:
        log.info("No processos discovered — skipping atividades.")
        ctx.cache["atividades_summary"] = {"atividades_loaded": 0, "failed_chunks": 0}
        return

    writer = ctx.require_writer()
    workers = int(ctx.flags.get("workers") or ctx.settings.workers or 8)
    chunk_size = int(ctx.flags.get("chunk_size") or 200)
    batch_size = int(ctx.flags.get("batch_size") or ctx.settings.batch_size or 500)

    chunks = [protocolo_ids[i:i + chunk_size] for i in range(0, len(protocolo_ids), chunk_size)]
    log.info(
        "atividades: %d processos in %d chunks, %d workers",
        len(protocolo_ids), len(chunks), workers,
    )

    grand_total = 0
    grand_stats: Counter = Counter()
    all_unclassified: list[str] = []
    completed_chunks = 0
    failed_chunks = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(process_chunk, chunk, writer, False, batch_size, ctx.settings): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            chunk_idx = futures[future]
            try:
                count, stats, unclassified, _units = future.result()
                grand_total += count
                grand_stats += stats
                for desc in unclassified:
                    if desc not in all_unclassified and len(all_unclassified) < 50:
                        all_unclassified.append(desc)
                completed_chunks += 1
                if completed_chunks % 10 == 0 or completed_chunks == len(chunks):
                    log.info(
                        "  Progress: %d/%d chunks (%d rows)",
                        completed_chunks, len(chunks), grand_total,
                    )
            except Exception:
                log.exception("Failed chunk %d", chunk_idx)
                failed_chunks += 1

    log.info("atividades complete: %d rows, %d failed chunks", grand_total, failed_chunks)
    if grand_stats:
        log.info("Classification distribution:")
        for tipo, count in grand_stats.most_common():
            log.info("  %-45s %6d", tipo, count)

    ctx.cache["atividades_summary"] = {
        "atividades_loaded": grand_total,
        "failed_chunks": failed_chunks,
        "chunks": len(chunks),
    }
