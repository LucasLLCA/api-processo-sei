"""Aggregate per-Processo features for clustering.

Composes a single dense vector per processo by combining:

- **Doc-rank-weighted semantic embedding** — first document gets weight 3.0
  (it usually defines the object of the request), early docs get 1.5, the
  rest 1.0. The result is L2-normalized.
- **Unigram TF-IDF over visited units** — binary TF (1/0, ignoring duration
  which can bias even within the same category), corpus-wide IDF discovers
  "key units" automatically.
- **Bigram TF-IDF over chronological unit sequence** — captures process
  flow (e.g. "RH → GAB" vs "CGE → JURIDICO") even when units repeat in
  different orders. Consecutive duplicates are collapsed (A,A,B,B,C → A,B,C).
- **Tipo-acao histogram** — normalized frequency of each task type, fixed
  vocabulary from ``pipeline.classifier.TASK_GROUPS``.

Concatenated vectors are reduced via UMAP to a configurable number of
dimensions (default 50) and persisted on the ``:Processo`` node so the
clustering stage can iterate over them without recomputing.

This module is pure-Python and side-effect-free: it takes lists of dicts
in, returns numpy arrays out. The stage runner in
``pipeline.stages.processo_embed`` wires it to Neo4j I/O.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from typing import Iterable, Sequence

import numpy as np

from .classifier import TASK_GROUPS

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Versioning — bump when the algorithm changes so cached cluster_features
# get invalidated automatically (their stored hash incorporates this).
# ---------------------------------------------------------------------------
ALGO_VERSION = "1.0"


# ---------------------------------------------------------------------------
# Tipo-acao vocabulary (fixed dim across runs)
# ---------------------------------------------------------------------------
def _tipo_acao_vocab() -> list[str]:
    """All task codes from ``classifier.TASK_GROUPS`` plus ``OUTROS``.

    Stable order = sorted by group then alphabetical. Used as a fixed-dim
    vocabulary so the histogram dimension never shifts.
    """
    out: list[str] = []
    for group_key in sorted(TASK_GROUPS.keys()):
        for task in sorted(TASK_GROUPS[group_key]["tasks"]):
            out.append(task)
    out.append("OUTROS")
    return out


TIPO_ACAO_VOCAB = _tipo_acao_vocab()
TIPO_ACAO_DIM = len(TIPO_ACAO_VOCAB)
_TIPO_ACAO_INDEX = {t: i for i, t in enumerate(TIPO_ACAO_VOCAB)}


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------
@dataclass
class ProcessoInput:
    """One processo's raw signals, as collected by the stage runner."""

    protocolo_formatado: str
    # Documents in chronological order (by min(atividade.data_hora) referencing
    # this doc). Each item: {numero, embedding (list[float]), embedding_hash}.
    documentos: list[dict]
    # Multiset of unit siglas the processo visited (de-duplicated for unigram
    # TF; chronological for bigram). Each item: {sigla, data_hora_first}.
    unidades_visitadas: list[str]            # set semantics for unigram
    unidades_cronologicas: list[str]         # in time order, NOT deduped
    # Histogram input: list of tipo_acao strings (one per atividade).
    atividades_tipo_acao: list[str]


@dataclass
class FeatureWeights:
    first_doc_weight: float = 3.0
    early_doc_weight: float = 1.5
    early_doc_until_rank: int = 3      # ranks 2-3 use early_doc_weight
    default_doc_weight: float = 1.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _l2_normalize(v: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(v)
    return v / n if n > 0 else v


def _doc_weight(rank_one_based: int, w: FeatureWeights) -> float:
    if rank_one_based == 1:
        return w.first_doc_weight
    if rank_one_based <= w.early_doc_until_rank:
        return w.early_doc_weight
    return w.default_doc_weight


def _dedup_consecutive(seq: list[str]) -> list[str]:
    """Collapse runs of identical adjacent elements: A,A,B,B,B,C → A,B,C."""
    out: list[str] = []
    for x in seq:
        if not x:
            continue
        if not out or out[-1] != x:
            out.append(x)
    return out


def _bigram_tokens(seq_dedup: list[str]) -> list[str]:
    """Form bigram tokens 'a__b' from a deduplicated chronological sequence.

    Using a separator string (rather than tuples) keeps sklearn's
    ``TfidfVectorizer`` happy with ``analyzer='word'``.
    """
    return [f"{a}__{b}" for a, b in zip(seq_dedup[:-1], seq_dedup[1:])]


# ---------------------------------------------------------------------------
# Per-processo feature computation
# ---------------------------------------------------------------------------
def semantic_embedding(processo: ProcessoInput, weights: FeatureWeights | None = None) -> np.ndarray:
    """Doc-rank-weighted, L2-normalized aggregate of document embeddings.

    Returns a 1D numpy array. If the processo has no documents with
    embeddings, returns a zero vector — caller decides whether to skip it.
    The dimension is inferred from the first non-empty embedding.
    """
    weights = weights or FeatureWeights()
    docs = [d for d in processo.documentos if d.get("embedding")]
    if not docs:
        return np.zeros(0, dtype=np.float32)

    dim = len(docs[0]["embedding"])
    acc = np.zeros(dim, dtype=np.float64)
    total_w = 0.0
    for rank, d in enumerate(docs, start=1):
        emb = np.asarray(d["embedding"], dtype=np.float64)
        if emb.shape != (dim,):
            log.warning(
                "doc %s embedding dim mismatch (%d vs %d) — skipping",
                d.get("numero"), emb.shape[0] if emb.size else 0, dim,
            )
            continue
        w = _doc_weight(rank, weights)
        acc += w * emb
        total_w += w
    if total_w == 0.0:
        return np.zeros(dim, dtype=np.float32)
    return _l2_normalize(acc / total_w).astype(np.float32)


def tipo_acao_histogram(processo: ProcessoInput) -> np.ndarray:
    """Normalized frequency vector over the fixed ``TIPO_ACAO_VOCAB``.

    Sum of returned vector equals 1.0 when the processo has any
    significant activities; otherwise the zero vector.
    """
    hist = np.zeros(TIPO_ACAO_DIM, dtype=np.float32)
    n = 0
    for tipo in processo.atividades_tipo_acao:
        idx = _TIPO_ACAO_INDEX.get(tipo, _TIPO_ACAO_INDEX["OUTROS"])
        hist[idx] += 1
        n += 1
    if n > 0:
        hist /= n
    return hist


def unidade_unigram_text(processo: ProcessoInput) -> str:
    """Stringification consumed by sklearn's TfidfVectorizer for unigrams.

    Deterministic ordering (sorted) — TF will be binary anyway.
    """
    return " ".join(sorted(set(processo.unidades_visitadas)))


def unidade_bigram_text(processo: ProcessoInput) -> str:
    """Stringification consumed by sklearn's TfidfVectorizer for bigrams.

    Bigrams come from the chronological sequence with consecutive-duplicate
    collapsing. Empty string when fewer than 2 distinct adjacent units.
    """
    seq = _dedup_consecutive(list(processo.unidades_cronologicas))
    if len(seq) < 2:
        return ""
    return " ".join(_bigram_tokens(seq))


# ---------------------------------------------------------------------------
# Hash for idempotency
# ---------------------------------------------------------------------------
def feature_hash(processo: ProcessoInput, weights: FeatureWeights | None = None,
                 algo_version: str = ALGO_VERSION) -> str:
    """Stable hash so the embed stage can skip processos whose inputs didn't change.

    Inputs to the hash:
    - ALGO_VERSION (so bumping the algorithm invalidates everything)
    - weights snapshot (so changing weights invalidates everything)
    - per-doc embedding_hash (preferred) or doc numero+content fallback
    - sorted unique units, chronological sequence (deduped)
    - sorted tipo_acao counts
    """
    weights = weights or FeatureWeights()
    h = hashlib.sha256()
    h.update(algo_version.encode())
    h.update(b"|")
    h.update(repr((
        weights.first_doc_weight,
        weights.early_doc_weight,
        weights.early_doc_until_rank,
        weights.default_doc_weight,
    )).encode())
    h.update(b"|")
    for d in processo.documentos:
        h.update((d.get("embedding_hash") or d.get("numero") or "").encode())
        h.update(b"\x01")
    h.update(b"|")
    for u in sorted(set(processo.unidades_visitadas)):
        h.update(u.encode())
        h.update(b"\x02")
    h.update(b"|")
    seq = _dedup_consecutive(list(processo.unidades_cronologicas))
    for u in seq:
        h.update(u.encode())
        h.update(b"\x03")
    h.update(b"|")
    counts: dict[str, int] = {}
    for t in processo.atividades_tipo_acao:
        counts[t] = counts.get(t, 0) + 1
    for k in sorted(counts.keys()):
        h.update(f"{k}={counts[k]}".encode())
        h.update(b"\x04")
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Batch composition (TF-IDF needs corpus-wide context)
# ---------------------------------------------------------------------------
@dataclass
class BatchFeatures:
    """Composed feature matrix for a batch of processos, ready for UMAP."""
    protocolos: list[str]
    raw: np.ndarray                      # (N, D_raw)
    semantic_dim: int
    unigram_dim: int
    bigram_dim: int
    tipo_acao_dim: int
    unigram_vocab: list[str] = field(default_factory=list)
    bigram_vocab: list[str] = field(default_factory=list)


def build_batch_features(
    processos: Sequence[ProcessoInput],
    *,
    weights: FeatureWeights | None = None,
    unigram_max_features: int = 500,
    bigram_max_features: int = 1000,
) -> BatchFeatures:
    """Compose the full per-processo feature matrix for a batch.

    The TF-IDF vectorizers are fit on the batch itself — that's the natural
    granularity for "this corpus" (the entire processo set being clustered).
    """
    from sklearn.feature_extraction.text import TfidfVectorizer

    weights = weights or FeatureWeights()

    # ── Semantic ──
    sem_blocks = [semantic_embedding(p, weights) for p in processos]
    # Determine the canonical semantic dim from the first non-empty vector
    sem_dim = 0
    for v in sem_blocks:
        if v.size > 0:
            sem_dim = v.shape[0]
            break
    # Pad zero-vectors to match the canonical dim so np.stack works
    sem_matrix = np.stack([
        v if v.size == sem_dim else np.zeros(sem_dim, dtype=np.float32)
        for v in sem_blocks
    ]) if sem_dim > 0 else np.zeros((len(processos), 0), dtype=np.float32)

    # ── Unigram TF-IDF (binary TF) ──
    unigram_texts = [unidade_unigram_text(p) for p in processos]
    if any(unigram_texts):
        v_uni = TfidfVectorizer(
            analyzer="word", token_pattern=r"\S+",
            binary=True, max_features=unigram_max_features, sublinear_tf=False,
        )
        uni_sparse = v_uni.fit_transform(unigram_texts)
        uni_dense = uni_sparse.toarray().astype(np.float32)
        uni_vocab = sorted(v_uni.vocabulary_, key=lambda k: v_uni.vocabulary_[k])
    else:
        uni_dense = np.zeros((len(processos), 0), dtype=np.float32)
        uni_vocab = []

    # ── Bigram TF-IDF over chronological unit pairs ──
    bigram_texts = [unidade_bigram_text(p) for p in processos]
    if any(bigram_texts):
        v_bi = TfidfVectorizer(
            analyzer="word", token_pattern=r"\S+",
            binary=False, max_features=bigram_max_features, sublinear_tf=False,
        )
        bi_sparse = v_bi.fit_transform(bigram_texts)
        bi_dense = bi_sparse.toarray().astype(np.float32)
        bi_vocab = sorted(v_bi.vocabulary_, key=lambda k: v_bi.vocabulary_[k])
    else:
        bi_dense = np.zeros((len(processos), 0), dtype=np.float32)
        bi_vocab = []

    # ── Tipo-acao histogram ──
    hist_matrix = np.stack([tipo_acao_histogram(p) for p in processos])

    # ── Concatenate ──
    raw = np.concatenate([sem_matrix, uni_dense, bi_dense, hist_matrix], axis=1)

    return BatchFeatures(
        protocolos=[p.protocolo_formatado for p in processos],
        raw=raw,
        semantic_dim=sem_matrix.shape[1],
        unigram_dim=uni_dense.shape[1],
        bigram_dim=bi_dense.shape[1],
        tipo_acao_dim=hist_matrix.shape[1],
        unigram_vocab=uni_vocab,
        bigram_vocab=bi_vocab,
    )


# ---------------------------------------------------------------------------
# UMAP reduction
# ---------------------------------------------------------------------------
def reduce_with_umap(
    batch: BatchFeatures,
    *,
    n_components: int = 50,
    metric: str = "cosine",
    n_neighbors: int = 15,
    random_state: int = 42,
) -> np.ndarray:
    """Reduce the raw feature matrix to ``n_components`` dimensions via UMAP.

    Determinism: ``random_state=42`` is honored, but UMAP is not strictly
    deterministic across CPU types / numba versions. Rerunning on the same
    machine gives byte-identical results; cross-machine variance is small
    and irrelevant to clustering quality at this scale.
    """
    import umap

    n_samples = batch.raw.shape[0]
    if n_samples == 0:
        return np.zeros((0, n_components), dtype=np.float32)

    # UMAP requires n_neighbors < n_samples; fall back gracefully on tiny
    # batches (mainly relevant in unit tests).
    effective_neighbors = min(n_neighbors, max(2, n_samples - 1))
    effective_components = min(n_components, max(1, n_samples - 1))

    reducer = umap.UMAP(
        n_components=effective_components,
        metric=metric,
        n_neighbors=effective_neighbors,
        random_state=random_state,
    )
    return reducer.fit_transform(batch.raw).astype(np.float32)
