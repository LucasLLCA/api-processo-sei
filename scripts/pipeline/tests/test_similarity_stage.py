"""Tests for ``pipeline.stages.similarity``: tier classification + cosine."""

from __future__ import annotations

from pipeline.stages.similarity import _cosine, _ordered_pair, classify_tier


# ---------------------------------------------------------------------------
# Tier classification — boundary points per the TCC spec
# ---------------------------------------------------------------------------
def test_tier_very_high_at_threshold():
    assert classify_tier(0.90) == "very_high"
    assert classify_tier(0.95) == "very_high"
    assert classify_tier(1.00) == "very_high"


def test_tier_high_just_below_very_high():
    assert classify_tier(0.899) == "high"
    assert classify_tier(0.85) == "high"
    assert classify_tier(0.80) == "high"


def test_tier_medium():
    assert classify_tier(0.799) == "medium"
    assert classify_tier(0.50) == "medium"
    assert classify_tier(0.40) == "medium"


def test_tier_low_persists_for_tcc():
    """TCC explicitly wants `low` persisted (not discarded)."""
    assert classify_tier(0.399) == "low"
    assert classify_tier(0.10) == "low"
    assert classify_tier(0.0) == "low"


# ---------------------------------------------------------------------------
# Cosine similarity
# ---------------------------------------------------------------------------
def test_cosine_identical_vectors_is_one():
    v = [0.1, 0.2, 0.3, 0.4]
    assert abs(_cosine(v, v) - 1.0) < 1e-9


def test_cosine_orthogonal_vectors_is_zero():
    assert abs(_cosine([1.0, 0.0], [0.0, 1.0])) < 1e-9


def test_cosine_opposite_vectors_is_minus_one():
    assert abs(_cosine([1.0, 0.0], [-1.0, 0.0]) - (-1.0)) < 1e-9


def test_cosine_handles_zero_vector_gracefully():
    assert _cosine([0.0, 0.0], [1.0, 1.0]) == 0.0
    assert _cosine([], [1.0]) == 0.0
    assert _cosine([1.0], [2.0, 3.0]) == 0.0


# ---------------------------------------------------------------------------
# Ordered pair — guarantees undirected edge dedup
# ---------------------------------------------------------------------------
def test_ordered_pair_lexicographic():
    assert _ordered_pair("BBB", "AAA") == ("AAA", "BBB")
    assert _ordered_pair("AAA", "BBB") == ("AAA", "BBB")


def test_ordered_pair_idempotent():
    a, b = _ordered_pair("doc-2", "doc-1")
    a2, b2 = _ordered_pair(b, a)
    assert (a, b) == (a2, b2)
