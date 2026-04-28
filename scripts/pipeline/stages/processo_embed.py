"""Stage ``processo-embed``: aggregate per-Processo cluster features.

Reads each :Processo's documents (with embeddings), visited unidades and
atividades from Neo4j, runs ``pipeline.processo_features`` to produce a
single dense vector per processo, reduces it via UMAP, and persists the
result on the :Processo node as ``cluster_features``.

Idempotency: a content hash (``feature_hash``) is computed from the
processo's input signals + algorithm version + weights. If the stored
hash already matches, the processo is skipped.
"""

from __future__ import annotations

import logging
from typing import Any

from ..cypher import (
    LIST_PROCESSOS_FOR_CLUSTERING_CYPHER,
    LOAD_PROCESSO_CLUSTER_FEATURES_CYPHER,
)
from ..processo_features import (
    ALGO_VERSION,
    FeatureWeights,
    ProcessoInput,
    build_batch_features,
    feature_hash,
    reduce_with_umap,
)
from ..registry import stage
from ..state import now_iso
from .._stage_base import RunContext, StageMeta

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------
@stage(StageMeta(
    name="processo-embed",
    description="Agrega doc embeddings + TF-IDF de unidades + tipo_acao + UMAP → :Processo.cluster_features.",
    type="enrich",
    depends_on=("embed",),
    soft_depends_on=("permanencia", "atividades"),
    modes=("neo4j", "json-emit"),
    estimated_duration="~5-10min para 12k processos com embeddings prontos",
))
def run(ctx: RunContext) -> None:
    driver = ctx.require_driver()
    writer = ctx.require_writer()

    weights = FeatureWeights(
        first_doc_weight=float(ctx.flags.get("first_doc_weight") or 3.0),
        early_doc_weight=float(ctx.flags.get("early_doc_weight") or 1.5),
        early_doc_until_rank=int(ctx.flags.get("early_doc_until_rank") or 3),
        default_doc_weight=float(ctx.flags.get("default_doc_weight") or 1.0),
    )
    n_components = int(ctx.flags.get("umap_n_components") or 50)
    unigram_max = int(ctx.flags.get("unigram_max_features") or 500)
    bigram_max = int(ctx.flags.get("bigram_max_features") or 1000)
    limit = int(ctx.flags.get("limit") or 0)
    force_all = bool(ctx.flags.get("force_all", False))

    log.info(
        "processo-embed: weights=%s, umap_n=%d, unigram_max=%d, bigram_max=%d",
        weights, n_components, unigram_max, bigram_max,
    )

    # ── Load all processos with their raw signals from Neo4j ──
    processos_input: list[ProcessoInput] = []
    existing_hash: dict[str, str] = {}

    with driver.session() as session:
        result = session.run(LIST_PROCESSOS_FOR_CLUSTERING_CYPHER)
        for record in result:
            pf = record["protocolo"]
            existing_hash[pf] = record.get("cluster_features_hash") or ""

            # Documents are returned with first_seen — sort Python-side
            # (avoids depending on apoc.coll.sortMaps).
            docs_raw = list(record.get("documentos_raw") or [])
            docs_raw.sort(key=lambda d: d.get("first_seen") or "")
            documentos = [
                {"numero": d["numero"], "embedding": d["embedding"],
                 "embedding_hash": d.get("embedding_hash") or ""}
                for d in docs_raw if d and d.get("embedding")
            ]

            # Unidades — set semantics for unigram, chronological for bigram.
            # `unidade_events_raw` is the raw {u, ts} list; sort by ts then
            # extract just the unit names.
            events = list(record.get("unidade_events_raw") or [])
            events.sort(key=lambda ev: ev.get("ts") or "")
            cronologia = [ev["u"] for ev in events if ev.get("u")]
            unidades = list(record.get("unidades_visitadas") or [])

            tipos = list(record.get("atividades_tipo_acao") or [])
            processos_input.append(ProcessoInput(
                protocolo_formatado=pf,
                documentos=documentos,
                unidades_visitadas=unidades,
                unidades_cronologicas=cronologia,
                atividades_tipo_acao=tipos,
            ))
            if limit and len(processos_input) >= limit:
                break

    log.info("processo-embed: %d processos loaded for feature aggregation", len(processos_input))

    if not processos_input:
        ctx.cache["processo_embed_summary"] = {"total": 0, "computed": 0, "skipped": 0}
        return

    # ── Filter out processos whose hash already matches (idempotency) ──
    pending: list[ProcessoInput] = []
    skipped = 0
    for p in processos_input:
        h = feature_hash(p, weights, ALGO_VERSION)
        if not force_all and existing_hash.get(p.protocolo_formatado) == h:
            skipped += 1
            continue
        pending.append(p)

    log.info("processo-embed: %d pending, %d skipped (hash match)", len(pending), skipped)
    if not pending:
        ctx.cache["processo_embed_summary"] = {
            "total": len(processos_input), "computed": 0, "skipped": skipped,
        }
        return

    # ── Build raw features (TF-IDF over the batch) ──
    log.info("processo-embed: composing raw feature matrix...")
    batch = build_batch_features(
        pending, weights=weights,
        unigram_max_features=unigram_max,
        bigram_max_features=bigram_max,
    )
    log.info(
        "processo-embed: raw shape=%s (semantic=%d, unigram=%d, bigram=%d, tipo_acao=%d)",
        batch.raw.shape, batch.semantic_dim, batch.unigram_dim,
        batch.bigram_dim, batch.tipo_acao_dim,
    )

    # ── UMAP reduction ──
    log.info("processo-embed: running UMAP → %d dims", n_components)
    reduced = reduce_with_umap(
        batch, n_components=n_components,
        random_state=int(ctx.flags.get("umap_random_state") or 42),
    )

    # ── Persist on :Processo ──
    writer.open_phase("processo-embed")
    try:
        ts = now_iso()
        rows = []
        for i, p in enumerate(pending):
            h = feature_hash(p, weights, ALGO_VERSION)
            rows.append({
                "protocolo_formatado": p.protocolo_formatado,
                "features": reduced[i].tolist(),
                "dim": int(reduced.shape[1]),
                "hash": h,
                "computed_at": ts,
            })
        for i in range(0, len(rows), 200):
            writer.execute_template(
                "load_processo_cluster_features",
                LOAD_PROCESSO_CLUSTER_FEATURES_CYPHER,
                {"rows": rows[i:i + 200]},
                phase="processo-embed",
            )
    finally:
        writer.close_phase("processo-embed")

    summary = {
        "total": len(processos_input),
        "computed": len(pending),
        "skipped": skipped,
        "umap_dim": int(reduced.shape[1]),
        "raw_dim": int(batch.raw.shape[1]),
    }
    ctx.cache["processo_embed_summary"] = summary
    log.info("processo-embed complete: %s", summary)
