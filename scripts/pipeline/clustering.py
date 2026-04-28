"""Hierarchical clustering of processos: HDBSCAN macro + micro + LLM naming.

Pure-Python algorithm working on a (N, D) numpy matrix of processo
``cluster_features`` (the UMAP-reduced vectors persisted by stage
``processo-embed``). Produces:

- ``ClusterAssignment`` per processo: macro_cluster_id, macro_strength,
  micro_cluster_id, micro_strength, ``reclassificada`` (true when the
  processo was a macro-outlier and rescued by the micro pass over the
  outlier bucket).
- ``ClusterCatalog`` describing each (macro, micro) cluster with its
  centroid and member count, ready to be named by the LLM and persisted
  as ``:CategoriaProcesso`` nodes.

The LLM-naming side is split into ``name_clusters`` so it can be unit
tested with a mock client.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence

import numpy as np

log = logging.getLogger(__name__)


# Sentinel for HDBSCAN outliers
NOISE_LABEL = -1

# IDs in :CategoriaProcesso are deterministic from (run_id, scope, label),
# so re-runs with the same input produce the same node IDs.
def _category_id(run_id: str, level: str, label: int, parent_label: int | None = None) -> str:
    if parent_label is None:
        return f"{run_id}|{level}|{label}"
    return f"{run_id}|{level}|{parent_label}|{label}"


# ---------------------------------------------------------------------------
# Data containers
# ---------------------------------------------------------------------------
@dataclass
class ClusterAssignment:
    """Per-processo result of the macro+micro pipeline."""

    protocolo: str
    macro_label: int            # NOISE_LABEL when no macro cluster fits
    macro_strength: float       # 0..1, HDBSCAN membership probability
    micro_label: int            # NOISE_LABEL when no micro cluster fits
    micro_strength: float
    reclassificada: bool        # True when rescued from the macro-outlier bucket


@dataclass
class ClusterDescriptor:
    """One (macro, micro) cluster's identity + content for naming."""

    macro_label: int
    micro_label: int | None      # None when describing the macro-only level
    nivel: str                    # "macro" | "micro" | "reclassificada"
    member_protocolos: list[str]
    centroid: np.ndarray         # mean of feature vectors in the cluster
    sample_ranks: list[int] = field(default_factory=list)   # indices sorted by strength desc


@dataclass
class ClusterCatalog:
    """All clusters produced by one run, plus per-processo assignments."""

    run_id: str
    macro_clusters: list[ClusterDescriptor]
    micro_clusters: list[ClusterDescriptor]
    assignments: dict[str, ClusterAssignment]   # keyed by protocolo


# ---------------------------------------------------------------------------
# HDBSCAN passes
# ---------------------------------------------------------------------------
def _hdbscan_fit(features: np.ndarray, min_cluster_size: int):
    """Wrapper that handles edge cases (too few points to cluster).

    HDBSCAN requires float64 input — float32 + numpy 2.x interactions can
    cause silent all-noise outputs. We coerce here so callers don't have
    to think about it.
    """
    import hdbscan
    n = features.shape[0]
    if n < max(2, min_cluster_size):
        # Not enough points — every member is noise
        labels = np.full(n, NOISE_LABEL, dtype=int)
        strengths = np.zeros(n, dtype=float)
        return labels, strengths
    feat64 = features.astype(np.float64, copy=False)
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        metric="euclidean",
        prediction_data=False,
    )
    labels = clusterer.fit_predict(feat64)
    strengths = clusterer.probabilities_
    return labels, strengths


def cluster_macro(features: np.ndarray, *, min_cluster_size: int = 20):
    """First pass: macro clusters. Returns (labels, strengths)."""
    return _hdbscan_fit(features, min_cluster_size)


def cluster_micro_within(
    features: np.ndarray,
    macro_labels: np.ndarray,
    *,
    min_cluster_size: int = 5,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Second pass: re-cluster each macro group + the outlier bucket separately.

    Returns:
        micro_labels — per-row label scoped within each macro group; -1 for noise.
        micro_strengths — membership probabilities.
        reclassificada — boolean array, True when the row was a macro-outlier
        that got picked up by the outlier-bucket micro pass.
    """
    n = features.shape[0]
    micro_labels = np.full(n, NOISE_LABEL, dtype=int)
    micro_strengths = np.zeros(n, dtype=float)
    reclassificada = np.zeros(n, dtype=bool)

    unique_macros = sorted({int(x) for x in macro_labels if int(x) != NOISE_LABEL})
    for m in unique_macros:
        idx = np.where(macro_labels == m)[0]
        sub = features[idx]
        sub_labels, sub_str = _hdbscan_fit(sub, min_cluster_size)
        micro_labels[idx] = sub_labels
        micro_strengths[idx] = sub_str

    # Separate pass on macro-outliers — rescues misses by lowering granularity.
    outlier_idx = np.where(macro_labels == NOISE_LABEL)[0]
    if outlier_idx.size > 0:
        sub = features[outlier_idx]
        sub_labels, sub_str = _hdbscan_fit(sub, min_cluster_size)
        micro_labels[outlier_idx] = sub_labels
        micro_strengths[outlier_idx] = sub_str
        # Anyone who landed in a real cluster here is a "rescue"
        reclassificada[outlier_idx] = sub_labels != NOISE_LABEL

    return micro_labels, micro_strengths, reclassificada


# ---------------------------------------------------------------------------
# Catalog assembly
# ---------------------------------------------------------------------------
def build_catalog(
    run_id: str,
    protocolos: Sequence[str],
    features: np.ndarray,
    macro_labels: np.ndarray,
    macro_strengths: np.ndarray,
    micro_labels: np.ndarray,
    micro_strengths: np.ndarray,
    reclassificada: np.ndarray,
) -> ClusterCatalog:
    """Roll up per-row arrays into ``ClusterDescriptor`` lists + assignments."""
    n = len(protocolos)
    assert features.shape[0] == n == macro_labels.shape[0] == micro_labels.shape[0]

    assignments: dict[str, ClusterAssignment] = {}
    for i, pf in enumerate(protocolos):
        assignments[pf] = ClusterAssignment(
            protocolo=pf,
            macro_label=int(macro_labels[i]),
            macro_strength=float(macro_strengths[i]),
            micro_label=int(micro_labels[i]),
            micro_strength=float(micro_strengths[i]),
            reclassificada=bool(reclassificada[i]),
        )

    macro_clusters: list[ClusterDescriptor] = []
    for m in sorted({int(x) for x in macro_labels if int(x) != NOISE_LABEL}):
        idx = np.where(macro_labels == m)[0]
        # Sort members by strength desc — top members are LLM samples
        sort_order = idx[np.argsort(-macro_strengths[idx])]
        macro_clusters.append(ClusterDescriptor(
            macro_label=m, micro_label=None, nivel="macro",
            member_protocolos=[protocolos[i] for i in sort_order],
            centroid=features[idx].mean(axis=0),
            sample_ranks=[int(i) for i in sort_order],
        ))

    # Micro: index by (macro_or_outlier, micro). reclassificada-derived clusters
    # use macro_label=-1 as their parent; we surface them with nivel="reclassificada".
    micro_clusters: list[ClusterDescriptor] = []
    micro_keys: set[tuple[int, int]] = set()
    for i in range(n):
        m, sub = int(macro_labels[i]), int(micro_labels[i])
        if sub == NOISE_LABEL:
            continue
        micro_keys.add((m, sub))

    for (m, sub) in sorted(micro_keys):
        idx = np.where((macro_labels == m) & (micro_labels == sub))[0]
        sort_order = idx[np.argsort(-micro_strengths[idx])]
        nivel = "reclassificada" if m == NOISE_LABEL else "micro"
        micro_clusters.append(ClusterDescriptor(
            macro_label=m, micro_label=sub, nivel=nivel,
            member_protocolos=[protocolos[i] for i in sort_order],
            centroid=features[idx].mean(axis=0),
            sample_ranks=[int(i) for i in sort_order],
        ))

    return ClusterCatalog(
        run_id=run_id,
        macro_clusters=macro_clusters,
        micro_clusters=micro_clusters,
        assignments=assignments,
    )


# ---------------------------------------------------------------------------
# LLM naming
# ---------------------------------------------------------------------------
NAMING_SYSTEM_PROMPT = """\
Você categoriza processos administrativos do governo do Estado do Piauí (sistema SEI). \
Recebe uma amostra de processos que pertencem a uma mesma categoria emergente \
(detectada via clustering não-supervisionado). Sua tarefa é dar à categoria:

1. Um nome curto em pt-BR (no máximo 4 palavras, ex: "Contratação direta", \
"Pedido de férias", "Aposentadoria por invalidez", "Diligência interna").
2. Uma descrição em 1 frase, factual.
3. Três marcadores que distinguem esta categoria de outras (palavras-chave, \
tipo de unidade envolvida, tipo de documento, etc.).

Retorne APENAS JSON válido neste schema:

{
  "nome": "string",
  "descricao": "string",
  "marcadores": ["string", "string", "string"]
}
"""


def _format_sample(p: dict) -> str:
    """Format one processo's sample data for the LLM prompt."""
    parts = [
        f"- protocolo: {p.get('protocolo','')}",
        f"  resumo: {p.get('resumo','(sem resumo)')[:500]}",
    ]
    if p.get("primeiro_doc_texto"):
        parts.append(f"  primeiro_doc: {p['primeiro_doc_texto'][:300]}")
    if p.get("top_unidades"):
        parts.append(f"  top_unidades: {', '.join(p['top_unidades'])}")
    if p.get("top_tipos_acao"):
        parts.append(f"  top_tipos_acao: {', '.join(p['top_tipos_acao'])}")
    return "\n".join(parts)


def name_cluster(
    samples: list[dict],
    llm_call: Callable[[list[dict[str, str]]], str],
) -> dict[str, Any]:
    """Ask the LLM to name a cluster given representative samples.

    ``llm_call`` takes a list of OpenAI-style messages and returns the
    assistant's content as a string. Decoupled like this so the unit tests
    can mock without an HTTP client.

    Returns ``{"nome", "descricao", "marcadores"}``. On invalid JSON,
    returns a defensive default with ``nome="Cluster sem nome"``.
    """
    user_msg = (
        f"Amostra de {len(samples)} processos desta categoria:\n\n"
        + "\n\n".join(_format_sample(s) for s in samples)
    )
    messages = [
        {"role": "system", "content": NAMING_SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
    ]
    try:
        raw = llm_call(messages)
    except Exception as e:
        log.warning("LLM naming call failed: %s — using defensive default", e)
        return _default_name(samples)

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        log.warning("LLM naming returned invalid JSON: %s", raw[:200])
        return _default_name(samples)

    nome = (data.get("nome") or "").strip()
    descricao = (data.get("descricao") or "").strip()
    marcadores = data.get("marcadores") or []
    if not nome:
        return _default_name(samples)
    if not isinstance(marcadores, list):
        marcadores = []
    return {
        "nome": nome[:120],
        "descricao": descricao[:500],
        "marcadores": [str(m)[:80] for m in marcadores][:5],
    }


def _default_name(samples: list[dict]) -> dict[str, Any]:
    return {
        "nome": "Cluster sem nome",
        "descricao": f"Categoria emergente com {len(samples)} processos.",
        "marcadores": [],
    }


def name_all_clusters(
    catalog: ClusterCatalog,
    sample_loader: Callable[[list[str]], list[dict]],
    llm_call: Callable[[list[dict[str, str]]], str],
    *,
    samples_per_cluster: int = 5,
) -> dict[str, dict[str, Any]]:
    """Name every cluster in the catalog. Keyed by category id.

    ``sample_loader`` turns a list of protocolos into a list of sample
    dicts (resumo + top_unidades + …). The runner stage provides one that
    queries Neo4j; tests provide a stub.
    """
    out: dict[str, dict[str, Any]] = {}
    run_id = catalog.run_id

    for descriptor in catalog.macro_clusters:
        cid = _category_id(run_id, "macro", descriptor.macro_label)
        sample_protos = descriptor.member_protocolos[:samples_per_cluster]
        samples = sample_loader(sample_protos)
        out[cid] = name_cluster(samples, llm_call)

    for descriptor in catalog.micro_clusters:
        cid = _category_id(
            run_id,
            descriptor.nivel,                   # "micro" or "reclassificada"
            descriptor.micro_label,
            descriptor.macro_label,
        )
        sample_protos = descriptor.member_protocolos[:samples_per_cluster]
        samples = sample_loader(sample_protos)
        out[cid] = name_cluster(samples, llm_call)

    return out


# ---------------------------------------------------------------------------
# Public id helper (re-exported for the stage runner)
# ---------------------------------------------------------------------------
def category_id(run_id: str, level: str, label: int, parent_label: int | None = None) -> str:
    """Stable composite id for a :CategoriaProcesso node."""
    return _category_id(run_id, level, label, parent_label)
