"""Tests for ``pipeline.processo_features`` aggregator."""

from __future__ import annotations

import numpy as np

from pipeline.processo_features import (
    ALGO_VERSION,
    TIPO_ACAO_DIM,
    BatchFeatures,
    FeatureWeights,
    ProcessoInput,
    _bigram_tokens,
    _dedup_consecutive,
    build_batch_features,
    feature_hash,
    reduce_with_umap,
    semantic_embedding,
    tipo_acao_histogram,
    unidade_bigram_text,
    unidade_unigram_text,
)


def _proc(protocolo, docs=None, unidades=None, cronologia=None, acoes=None):
    return ProcessoInput(
        protocolo_formatado=protocolo,
        documentos=docs or [],
        unidades_visitadas=unidades or [],
        unidades_cronologicas=cronologia or [],
        atividades_tipo_acao=acoes or [],
    )


def _doc(numero, embedding):
    return {"numero": numero, "embedding": list(embedding), "embedding_hash": f"h-{numero}"}


# ---------------------------------------------------------------------------
# Doc-rank weighting — first doc dominates the aggregate
# ---------------------------------------------------------------------------
def test_first_doc_weighted_more_than_followups():
    """First doc has weight 3× higher than rank-4+ docs. Verify by comparing
    weighted vs unweighted aggregation on the same input."""
    p = _proc("P1", docs=[
        _doc("d1", [1.0, 0.0]),       # rank 1
        _doc("d2", [0.0, 1.0]),       # rank 2
        _doc("d3", [0.0, 1.0]),       # rank 3
        _doc("d4", [0.0, 1.0]),       # rank 4
    ])
    weighted = semantic_embedding(p)  # default weights (first=3, early=1.5)
    unweighted = semantic_embedding(p, FeatureWeights(
        first_doc_weight=1.0, early_doc_weight=1.0, default_doc_weight=1.0,
    ))
    # Weighted aggregation must lean MORE toward d1 (axis 0) than unweighted.
    assert weighted[0] > unweighted[0]


def test_single_doc_returns_normalized_embedding():
    p = _proc("P1", docs=[_doc("d1", [3.0, 4.0])])
    emb = semantic_embedding(p)
    # L2-normalized: 3/5, 4/5
    assert np.isclose(emb[0], 0.6, atol=1e-6)
    assert np.isclose(emb[1], 0.8, atol=1e-6)


def test_no_docs_returns_empty_zero():
    p = _proc("P1", docs=[])
    emb = semantic_embedding(p)
    assert emb.size == 0


def test_doc_with_dim_mismatch_is_skipped():
    p = _proc("P1", docs=[
        _doc("d1", [1.0, 0.0]),       # canonical dim = 2
        _doc("d2", [1.0, 0.0, 0.0]),  # dim 3 — skipped
    ])
    emb = semantic_embedding(p)
    assert emb.shape == (2,)


def test_custom_weights_change_aggregation():
    p = _proc("P1", docs=[
        _doc("d1", [1.0, 0.0]),
        _doc("d2", [0.0, 1.0]),
    ])
    eq_w = FeatureWeights(first_doc_weight=1.0, early_doc_weight=1.0, default_doc_weight=1.0)
    biased = FeatureWeights(first_doc_weight=10.0, early_doc_weight=1.0, default_doc_weight=1.0)
    eq = semantic_embedding(p, eq_w)
    bs = semantic_embedding(p, biased)
    assert bs[0] > eq[0]


# ---------------------------------------------------------------------------
# Tipo-acao histogram
# ---------------------------------------------------------------------------
def test_tipo_acao_histogram_normalizes_to_one():
    p = _proc("P1", acoes=["GERACAO-PROCEDIMENTO", "PROCESSO-RECEBIDO-UNIDADE",
                            "PROCESSO-RECEBIDO-UNIDADE", "GERACAO-DOCUMENTO"])
    h = tipo_acao_histogram(p)
    assert h.shape == (TIPO_ACAO_DIM,)
    assert np.isclose(h.sum(), 1.0)


def test_tipo_acao_unknown_falls_to_outros():
    p = _proc("P1", acoes=["TOTALMENTE-INVENTADO"])
    h = tipo_acao_histogram(p)
    assert np.isclose(h.sum(), 1.0)
    # OUTROS is the last index
    assert np.isclose(h[-1], 1.0)


def test_tipo_acao_empty_returns_zero_vector():
    p = _proc("P1", acoes=[])
    h = tipo_acao_histogram(p)
    assert h.sum() == 0.0


# ---------------------------------------------------------------------------
# Unidade unigram / bigram tokenization
# ---------------------------------------------------------------------------
def test_unigram_text_dedupes_visited_units():
    p = _proc("P1", unidades=["A", "B", "A", "C"])
    txt = unidade_unigram_text(p)
    parts = txt.split()
    assert sorted(parts) == sorted(["A", "B", "C"])


def test_bigram_text_uses_chronological_dedup():
    p = _proc("P1", cronologia=["A", "A", "B", "B", "B", "C", "A"])
    txt = unidade_bigram_text(p)
    # Dedup → A,B,C,A → bigrams: A__B, B__C, C__A
    assert txt == "A__B B__C C__A"


def test_bigram_text_empty_when_no_transitions():
    p = _proc("P1", cronologia=["A", "A", "A"])
    assert unidade_bigram_text(p) == ""


def test_dedup_consecutive_helper():
    assert _dedup_consecutive(["A", "A", "B", "B", "B", "C"]) == ["A", "B", "C"]
    assert _dedup_consecutive([]) == []


def test_bigram_tokens_helper():
    assert _bigram_tokens(["A", "B", "C"]) == ["A__B", "B__C"]
    assert _bigram_tokens(["A"]) == []


# ---------------------------------------------------------------------------
# Hash (idempotency)
# ---------------------------------------------------------------------------
def test_feature_hash_deterministic():
    p = _proc("P1",
              docs=[_doc("d1", [1.0])],
              unidades=["A", "B"],
              cronologia=["A", "B"],
              acoes=["GERACAO-PROCEDIMENTO"])
    h1 = feature_hash(p)
    h2 = feature_hash(p)
    assert h1 == h2


def test_feature_hash_changes_when_doc_changes():
    p1 = _proc("P1", docs=[_doc("d1", [1.0])])
    p2 = _proc("P1", docs=[_doc("d2", [1.0])])
    assert feature_hash(p1) != feature_hash(p2)


def test_feature_hash_changes_when_units_change():
    p1 = _proc("P1", unidades=["A"])
    p2 = _proc("P1", unidades=["A", "B"])
    assert feature_hash(p1) != feature_hash(p2)


def test_feature_hash_changes_when_weights_change():
    p = _proc("P1", docs=[_doc("d1", [1.0])])
    h1 = feature_hash(p, FeatureWeights(first_doc_weight=3.0))
    h2 = feature_hash(p, FeatureWeights(first_doc_weight=5.0))
    assert h1 != h2


def test_feature_hash_changes_when_algo_version_changes():
    p = _proc("P1", docs=[_doc("d1", [1.0])])
    h1 = feature_hash(p, algo_version="1.0")
    h2 = feature_hash(p, algo_version="2.0")
    assert h1 != h2


def test_feature_hash_independent_of_unit_order_unigram():
    p1 = _proc("P1", unidades=["A", "B", "C"])
    p2 = _proc("P1", unidades=["C", "B", "A"])
    # Unigram side uses sorted set, so hashes match for unigram inputs;
    # but cronologia order DOES matter — keep cronologia same.
    assert feature_hash(p1) == feature_hash(p2)


# ---------------------------------------------------------------------------
# Batch composition
# ---------------------------------------------------------------------------
def _make_batch(n: int) -> list[ProcessoInput]:
    """Build a small synthetic batch of N processos with overlapping units."""
    out: list[ProcessoInput] = []
    for i in range(n):
        out.append(_proc(
            f"P{i}",
            docs=[_doc(f"d-{i}-1", [1.0 if i % 2 == 0 else 0.0,
                                    0.0 if i % 2 == 0 else 1.0,
                                    0.5])],
            unidades=["UA", "UB"] if i % 2 == 0 else ["UC", "UD"],
            cronologia=["UA", "UB"] if i % 2 == 0 else ["UC", "UD"],
            acoes=["GERACAO-PROCEDIMENTO", "PROCESSO-RECEBIDO-UNIDADE"],
        ))
    return out


def test_build_batch_features_shape_consistency():
    procs = _make_batch(6)
    batch = build_batch_features(procs)
    assert isinstance(batch, BatchFeatures)
    assert batch.raw.shape[0] == 6
    # Concatenation: semantic + unigram + bigram + tipo_acao
    expected_cols = (
        batch.semantic_dim + batch.unigram_dim + batch.bigram_dim + batch.tipo_acao_dim
    )
    assert batch.raw.shape[1] == expected_cols


def test_build_batch_features_with_no_docs_works():
    procs = [_proc("P1", unidades=["A"], cronologia=["A"])]
    batch = build_batch_features(procs)
    assert batch.semantic_dim == 0
    # Still must have unigram + tipo_acao at least
    assert batch.raw.shape == (1, batch.unigram_dim + batch.bigram_dim + batch.tipo_acao_dim)


# ---------------------------------------------------------------------------
# UMAP reduction
# ---------------------------------------------------------------------------
def test_reduce_with_umap_handles_tiny_batch():
    procs = _make_batch(6)
    batch = build_batch_features(procs)
    reduced = reduce_with_umap(batch, n_components=3, n_neighbors=3, random_state=42)
    assert reduced.shape == (6, 3)


def test_reduce_with_umap_empty_returns_empty():
    batch = BatchFeatures(protocolos=[], raw=np.zeros((0, 10), dtype=np.float32),
                          semantic_dim=0, unigram_dim=0, bigram_dim=0, tipo_acao_dim=10)
    reduced = reduce_with_umap(batch, n_components=5)
    assert reduced.shape == (0, 5)


def test_algo_version_constant_present():
    assert isinstance(ALGO_VERSION, str)
    assert len(ALGO_VERSION) > 0
