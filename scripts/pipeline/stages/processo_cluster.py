"""Stage ``processo-cluster``: hierarchical HDBSCAN clustering + LLM naming.

Reads ``:Processo.cluster_features`` from Neo4j, runs:

1. **Macro pass** — HDBSCAN with ``min_cluster_size`` (default 20)
   discovers macro categories.
2. **Micro pass** — for each macro cluster, re-cluster its members with a
   smaller ``min_cluster_size`` (default 5). Macro outliers also pass
   through a dedicated micro pass; rows rescued there are flagged
   ``reclassificada=True``.
3. **LLM naming** — for each (macro, micro) cluster, sample the top-K
   most-central processos and ask the LLM (``Qwen/Qwen3.6-35B-A3B``) for
   ``nome``, ``descricao``, ``marcadores`` in JSON.
4. **Persist** — drop existing ``:CategoriaProcesso`` nodes + ``:CATEGORIZADO_COMO``
   edges scoped to this run, then create the new graph.

Mode: ``neo4j`` only (needs the live driver for queries + writes).
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import numpy as np

from ..clustering import (
    NOISE_LABEL,
    build_catalog,
    category_id,
    cluster_macro,
    cluster_micro_within,
    name_all_clusters,
)
from ..cypher import (
    CLEAR_CATEGORIA_PROCESSO_CYPHER,
    LOAD_CATEGORIA_PROCESSO_CYPHER,
    LOAD_CATEGORIZADO_COMO_CYPHER,
    LOAD_SUBCATEGORIA_DE_CYPHER,
    READ_ALL_CLUSTER_FEATURES_CYPHER,
    SAMPLE_PROCESSOS_FOR_NAMING_CYPHER,
)
from ..ner_llm import NerLLM, config_from_settings
from ..registry import stage
from ..state import now_iso
from .._stage_base import RunContext, StageMeta

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# LLM call adapter
# ---------------------------------------------------------------------------
def _build_llm_call(settings):
    """Build an llm_call(messages) function that returns the assistant content."""
    cfg = config_from_settings(settings, model="Qwen/Qwen3.6-35B-A3B")
    llm = NerLLM(cfg)

    def _call(messages: list[dict[str, str]]) -> str:
        # Re-use the raw client for naming (different system prompt than NER).
        # We bypass NerLLM's _chat_json validator because the schema differs.
        resp = llm._client.chat.completions.create(
            model=cfg.model,
            messages=messages,
            temperature=0.2,
            response_format={"type": "json_object"},
        )
        return resp.choices[0].message.content or ""

    return _call


# ---------------------------------------------------------------------------
# Sample loader (queries Neo4j for naming context)
# ---------------------------------------------------------------------------
def _build_sample_loader(driver):
    def _load(protocolos: list[str]) -> list[dict[str, Any]]:
        if not protocolos:
            return []
        with driver.session() as session:
            result = session.run(SAMPLE_PROCESSOS_FOR_NAMING_CYPHER, protocolos=protocolos)
            return [dict(r) for r in result]

    return _load


# ---------------------------------------------------------------------------
# Stage runner
# ---------------------------------------------------------------------------
@stage(StageMeta(
    name="processo-cluster",
    description="Clustering hierárquico de processos (HDBSCAN macro+micro) + LLM naming + :CategoriaProcesso.",
    type="enrich",
    depends_on=("processo-embed",),
    modes=("neo4j",),
    estimated_duration="~5-15min HDBSCAN + 1-3min naming via Mandu",
))
def run(ctx: RunContext) -> None:
    driver = ctx.require_driver()
    writer = ctx.require_writer()

    macro_min = int(ctx.flags.get("macro_min_cluster_size") or 20)
    micro_min = int(ctx.flags.get("micro_min_cluster_size") or 5)
    samples_per_cluster = int(ctx.flags.get("samples_per_cluster") or 5)
    skip_naming = bool(ctx.flags.get("skip_naming", False))
    run_id = ctx.flags.get("run_id") or uuid.uuid4().hex[:12]

    log.info(
        "processo-cluster: macro_min=%d, micro_min=%d, samples=%d, run_id=%s",
        macro_min, micro_min, samples_per_cluster, run_id,
    )

    # ── 1. Read all processo cluster_features from Neo4j ──
    protocolos: list[str] = []
    feature_rows: list[list[float]] = []
    with driver.session() as session:
        for r in session.run(READ_ALL_CLUSTER_FEATURES_CYPHER):
            f = r.get("features")
            if not f:
                continue
            protocolos.append(r["protocolo"])
            feature_rows.append(list(f))

    if not feature_rows:
        log.error("processo-cluster: no :Processo nodes have cluster_features. Run processo-embed first.")
        ctx.cache["processo_cluster_summary"] = {"error": "no_features", "total": 0}
        return

    features = np.asarray(feature_rows, dtype=np.float64)
    log.info("processo-cluster: %d processos × %d dims loaded", *features.shape)

    # ── 2. Macro + micro passes ──
    log.info("processo-cluster: running HDBSCAN macro (min=%d)...", macro_min)
    macro_labels, macro_str = cluster_macro(features, min_cluster_size=macro_min)
    n_macro = len(set(int(x) for x in macro_labels) - {NOISE_LABEL})
    n_macro_outliers = int((macro_labels == NOISE_LABEL).sum())
    log.info("processo-cluster: macro found %d clusters + %d outliers", n_macro, n_macro_outliers)

    log.info("processo-cluster: running HDBSCAN micro per macro group (min=%d)...", micro_min)
    micro_labels, micro_str, reclassificada = cluster_micro_within(
        features, macro_labels, min_cluster_size=micro_min,
    )
    n_micro = len(set((int(m), int(s)) for m, s in zip(macro_labels, micro_labels)
                       if int(s) != NOISE_LABEL))
    n_rescued = int(reclassificada.sum())
    log.info(
        "processo-cluster: micro found %d sub-clusters; %d processos rescued (reclassificada=true)",
        n_micro, n_rescued,
    )

    catalog = build_catalog(
        run_id=run_id,
        protocolos=protocolos, features=features,
        macro_labels=macro_labels, macro_strengths=macro_str,
        micro_labels=micro_labels, micro_strengths=micro_str,
        reclassificada=reclassificada,
    )

    # ── 3. LLM naming ──
    cluster_names: dict[str, dict[str, Any]] = {}
    if skip_naming:
        log.info("processo-cluster: --flag skip_naming=true — using placeholder names")
        for desc in catalog.macro_clusters:
            cid = category_id(run_id, "macro", desc.macro_label)
            cluster_names[cid] = {
                "nome": f"Macro #{desc.macro_label}",
                "descricao": f"{len(desc.member_protocolos)} processos",
                "marcadores": [],
            }
        for desc in catalog.micro_clusters:
            cid = category_id(run_id, desc.nivel, desc.micro_label, desc.macro_label)
            cluster_names[cid] = {
                "nome": f"{desc.nivel} #{desc.macro_label}.{desc.micro_label}",
                "descricao": f"{len(desc.member_protocolos)} processos",
                "marcadores": [],
            }
    else:
        log.info("processo-cluster: naming %d clusters via LLM (Qwen/Qwen3.6-35B-A3B)...",
                 len(catalog.macro_clusters) + len(catalog.micro_clusters))
        loader = _build_sample_loader(driver)
        llm_call = _build_llm_call(ctx.settings)
        cluster_names = name_all_clusters(
            catalog, loader, llm_call,
            samples_per_cluster=samples_per_cluster,
        )

    # ── 4. Persist into Neo4j ──
    writer.open_phase("processo-cluster")
    try:
        # 4a. Wipe any prior categorias from this graph
        writer.execute_template(
            "clear_categoria_processo",
            CLEAR_CATEGORIA_PROCESSO_CYPHER,
            {}, phase="processo-cluster",
        )

        # 4b. Create :CategoriaProcesso nodes (macro first, then micro)
        category_rows: list[dict[str, Any]] = []
        ts = now_iso()
        for desc in catalog.macro_clusters:
            cid = category_id(run_id, "macro", desc.macro_label)
            naming = cluster_names.get(cid) or {}
            category_rows.append({
                "id": cid,
                "nome": naming.get("nome") or f"Macro #{desc.macro_label}",
                "descricao": naming.get("descricao") or "",
                "marcadores": naming.get("marcadores") or [],
                "nivel": "macro",
                "n_processos": len(desc.member_protocolos),
                "centroid": desc.centroid.tolist(),
                "computed_at": ts,
            })
        for desc in catalog.micro_clusters:
            cid = category_id(run_id, desc.nivel, desc.micro_label, desc.macro_label)
            naming = cluster_names.get(cid) or {}
            category_rows.append({
                "id": cid,
                "nome": naming.get("nome") or f"{desc.nivel} #{desc.macro_label}.{desc.micro_label}",
                "descricao": naming.get("descricao") or "",
                "marcadores": naming.get("marcadores") or [],
                "nivel": desc.nivel,
                "n_processos": len(desc.member_protocolos),
                "centroid": desc.centroid.tolist(),
                "computed_at": ts,
            })
        if category_rows:
            for i in range(0, len(category_rows), 200):
                writer.execute_template(
                    "load_categoria_processo",
                    LOAD_CATEGORIA_PROCESSO_CYPHER,
                    {"rows": category_rows[i:i + 200]},
                    phase="processo-cluster",
                )

        # 4c. SUBCATEGORIA_DE edges (micro → macro)
        sub_rows: list[dict[str, str]] = []
        for desc in catalog.micro_clusters:
            if desc.macro_label == NOISE_LABEL:
                # Reclassified clusters have no real macro parent — skip the edge
                continue
            sub_rows.append({
                "child_id": category_id(run_id, "micro", desc.micro_label, desc.macro_label),
                "parent_id": category_id(run_id, "macro", desc.macro_label),
            })
        if sub_rows:
            for i in range(0, len(sub_rows), 200):
                writer.execute_template(
                    "load_subcategoria_de",
                    LOAD_SUBCATEGORIA_DE_CYPHER,
                    {"rows": sub_rows[i:i + 200]},
                    phase="processo-cluster",
                )

        # 4d. CATEGORIZADO_COMO edges (one per processo per level)
        cat_rows: list[dict[str, Any]] = []
        for pf, a in catalog.assignments.items():
            # Macro edge: only if processo has a macro cluster (skip outliers
            # entirely — they will only have a micro edge if rescued).
            if a.macro_label != NOISE_LABEL:
                cat_rows.append({
                    "protocolo_formatado": pf,
                    "categoria_id": category_id(run_id, "macro", a.macro_label),
                    "nivel": "macro",
                    "reclassificada": False,
                    "confidence": a.macro_strength,
                    "computed_at": ts,
                })
            # Micro / reclassificada edge: only if rescued or mapped to a real micro
            if a.micro_label != NOISE_LABEL:
                nivel = "reclassificada" if a.reclassificada else "micro"
                cat_rows.append({
                    "protocolo_formatado": pf,
                    "categoria_id": category_id(run_id, nivel, a.micro_label, a.macro_label),
                    "nivel": nivel,
                    "reclassificada": a.reclassificada,
                    "confidence": a.micro_strength,
                    "computed_at": ts,
                })
        for i in range(0, len(cat_rows), 500):
            writer.execute_template(
                "load_categorizado_como",
                LOAD_CATEGORIZADO_COMO_CYPHER,
                {"rows": cat_rows[i:i + 500]},
                phase="processo-cluster",
            )
    finally:
        writer.close_phase("processo-cluster")

    summary = {
        "run_id": run_id,
        "total_processos": len(protocolos),
        "macro_clusters": n_macro,
        "macro_outliers": n_macro_outliers,
        "micro_clusters": n_micro,
        "reclassificados": n_rescued,
        "categorias_criadas": len(category_rows),
        "edges_criadas": len(cat_rows),
    }
    ctx.cache["processo_cluster_summary"] = summary
    log.info("processo-cluster complete: %s", summary)
