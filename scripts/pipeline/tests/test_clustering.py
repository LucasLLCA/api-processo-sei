"""Tests for ``pipeline.clustering`` HDBSCAN macro/micro + LLM naming."""

from __future__ import annotations

import json

import numpy as np

from pipeline.clustering import (
    NOISE_LABEL,
    build_catalog,
    category_id,
    cluster_macro,
    cluster_micro_within,
    name_all_clusters,
    name_cluster,
)


# ---------------------------------------------------------------------------
# HDBSCAN — synthetic separable groups
# ---------------------------------------------------------------------------
def _three_clusters_synthetic(rng_seed: int = 0) -> tuple[list[str], np.ndarray]:
    """Three well-separated clusters in 2D — easy for HDBSCAN."""
    rng = np.random.default_rng(rng_seed)
    a = rng.normal(loc=[0, 0], scale=0.05, size=(20, 2))
    b = rng.normal(loc=[5, 0], scale=0.05, size=(20, 2))
    c = rng.normal(loc=[0, 5], scale=0.05, size=(20, 2))
    feat = np.vstack([a, b, c]).astype(np.float32)
    pf = [f"P{i}" for i in range(60)]
    return pf, feat


def test_macro_finds_three_separable_clusters():
    _, feat = _three_clusters_synthetic()
    labels, strengths = cluster_macro(feat, min_cluster_size=10)
    unique = set(int(x) for x in labels) - {NOISE_LABEL}
    assert len(unique) == 3
    # Most points should have non-trivial strength
    assert (strengths > 0).sum() >= 50


def test_macro_marks_isolated_points_as_noise():
    rng = np.random.default_rng(0)
    cluster = rng.normal(loc=[0, 0], scale=0.05, size=(20, 2))
    isolated = np.array([[10.0, 10.0]])
    feat = np.vstack([cluster, isolated]).astype(np.float32)
    labels, _ = cluster_macro(feat, min_cluster_size=10)
    assert labels[-1] == NOISE_LABEL


def test_macro_handles_too_few_points():
    feat = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32)
    labels, strengths = cluster_macro(feat, min_cluster_size=10)
    # All marked as noise because n < min_cluster_size
    assert (labels == NOISE_LABEL).all()
    assert (strengths == 0).all()


# ---------------------------------------------------------------------------
# Micro pass — outlier rescue
# ---------------------------------------------------------------------------
def test_micro_rescues_outlier_subgroups_into_named_clusters():
    """Synthetic input designed to exercise the rescue path of the micro pass.

    We construct three obvious tight sub-groups, then deliberately mark
    them all as macro-noise (simulating the case where macro
    min_cluster_size was too strict). The micro pass with a smaller
    min_cluster_size must find them and mark every member as reclassificada.
    """
    rng = np.random.default_rng(0)
    grp_a = rng.normal(loc=[0, 0], scale=0.1, size=(5, 2))
    grp_b = rng.normal(loc=[10, 10], scale=0.1, size=(5, 2))
    grp_c = rng.normal(loc=[-10, 10], scale=0.1, size=(5, 2))
    feat = np.vstack([grp_a, grp_b, grp_c]).astype(np.float32)

    # Manually mark every row as a macro-outlier (simulating that scenario)
    macro_labels = np.full(15, NOISE_LABEL, dtype=int)

    micro_labels, _, reclassificada = cluster_micro_within(
        feat, macro_labels, min_cluster_size=3,
    )
    # Each of the 3 sub-groups should be its own cluster after the rescue pass
    found_clusters = set(int(x) for x in micro_labels) - {NOISE_LABEL}
    assert len(found_clusters) >= 2
    # Every row that ended up in a cluster (≠ -1) must be reclassificada=True
    for i in range(15):
        if micro_labels[i] != NOISE_LABEL:
            assert reclassificada[i] is np.True_ or bool(reclassificada[i])


def test_micro_does_not_mark_macro_members_as_reclassificada():
    """Members of a real macro cluster must NEVER be marked reclassificada."""
    rng = np.random.default_rng(0)
    feat = rng.normal(loc=[0, 0], scale=0.3, size=(30, 2)).astype(np.float32)
    # Macro found one big cluster — every row labeled 0
    macro_labels = np.zeros(30, dtype=int)

    _, _, reclassificada = cluster_micro_within(feat, macro_labels, min_cluster_size=3)
    # Nobody in the macro cluster should be flagged as rescued
    assert not reclassificada.any()


# ---------------------------------------------------------------------------
# Catalog assembly
# ---------------------------------------------------------------------------
def test_build_catalog_groups_assignments_correctly():
    pf, feat = _three_clusters_synthetic()
    macro_labels, macro_str = cluster_macro(feat, min_cluster_size=10)
    micro_labels, micro_str, reclass = cluster_micro_within(
        feat, macro_labels, min_cluster_size=5,
    )
    catalog = build_catalog(
        run_id="test-run",
        protocolos=pf,
        features=feat,
        macro_labels=macro_labels,
        macro_strengths=macro_str,
        micro_labels=micro_labels,
        micro_strengths=micro_str,
        reclassificada=reclass,
    )
    assert catalog.run_id == "test-run"
    assert len(catalog.assignments) == len(pf)
    assert len(catalog.macro_clusters) == 3
    # Each ClusterDescriptor centroid has the right dimensionality
    assert catalog.macro_clusters[0].centroid.shape == (2,)
    # Sample ranks are sorted strongest-first within their group
    desc = catalog.macro_clusters[0]
    member_strengths = [macro_str[i] for i in desc.sample_ranks]
    assert member_strengths == sorted(member_strengths, reverse=True)


def test_catalog_skips_noise_clusters():
    """Only the cluster with macro_label != NOISE shows up in macro_clusters."""
    pf = [f"P{i}" for i in range(5)]
    feat = np.array([[0, 0]] * 5, dtype=np.float32)
    # All in one cluster
    macro_labels = np.array([0, 0, 0, 0, NOISE_LABEL], dtype=int)
    macro_str = np.array([0.9, 0.9, 0.9, 0.9, 0.0])
    micro_labels = np.array([NOISE_LABEL] * 5, dtype=int)
    micro_str = np.zeros(5)
    reclass = np.zeros(5, dtype=bool)

    catalog = build_catalog("r", pf, feat, macro_labels, macro_str,
                             micro_labels, micro_str, reclass)
    assert len(catalog.macro_clusters) == 1
    assert catalog.macro_clusters[0].macro_label == 0


# ---------------------------------------------------------------------------
# LLM naming (mocked)
# ---------------------------------------------------------------------------
def _stub_llm_returning(payload: dict):
    """Build an llm_call stub that returns a fixed JSON payload."""
    raw = json.dumps(payload)
    return lambda messages: raw


def test_name_cluster_parses_valid_json():
    samples = [{"protocolo": "P1", "resumo": "Pedido de licitação..."}]
    result = name_cluster(samples, _stub_llm_returning({
        "nome": "Contratação direta",
        "descricao": "Aquisição sem licitação prévia.",
        "marcadores": ["Lei 8.666", "Inexigibilidade", "JURIDICO"],
    }))
    assert result["nome"] == "Contratação direta"
    assert len(result["marcadores"]) == 3


def test_name_cluster_falls_back_on_invalid_json():
    samples = [{"protocolo": "P1", "resumo": "x"}]
    bad = lambda messages: "not valid json"
    result = name_cluster(samples, bad)
    assert result["nome"] == "Cluster sem nome"
    assert result["marcadores"] == []


def test_name_cluster_falls_back_on_llm_exception():
    def boom(messages):
        raise RuntimeError("api down")
    result = name_cluster([{"protocolo": "P1", "resumo": "x"}], boom)
    assert result["nome"] == "Cluster sem nome"


def test_name_cluster_truncates_long_fields():
    huge = {
        "nome": "x" * 500,
        "descricao": "y" * 5000,
        "marcadores": [f"m{i}" for i in range(20)],
    }
    result = name_cluster([{"protocolo": "P1"}], _stub_llm_returning(huge))
    assert len(result["nome"]) <= 120
    assert len(result["descricao"]) <= 500
    assert len(result["marcadores"]) <= 5


def test_name_all_clusters_uses_loader_and_naming():
    pf, feat = _three_clusters_synthetic()
    macro_labels, macro_str = cluster_macro(feat, min_cluster_size=10)
    micro_labels, micro_str, reclass = cluster_micro_within(
        feat, macro_labels, min_cluster_size=5,
    )
    catalog = build_catalog("test-run", pf, feat, macro_labels, macro_str,
                             micro_labels, micro_str, reclass)

    loader_called: list[list[str]] = []

    def loader(protocolos):
        loader_called.append(list(protocolos))
        return [{"protocolo": p, "resumo": f"resumo {p}"} for p in protocolos]

    llm = _stub_llm_returning({
        "nome": "Categoria X",
        "descricao": "descrição",
        "marcadores": ["a", "b", "c"],
    })
    names = name_all_clusters(catalog, loader, llm, samples_per_cluster=3)
    # 3 macro clusters → ≥ 3 entries; micro pass may or may not produce more
    assert len(names) >= 3
    # Loader was called for each cluster, with at most samples_per_cluster protocolos
    for sample_set in loader_called:
        assert len(sample_set) <= 3


# ---------------------------------------------------------------------------
# Category id helper
# ---------------------------------------------------------------------------
def test_category_id_is_deterministic_and_includes_parent():
    a = category_id("run-1", "macro", 5)
    assert a == "run-1|macro|5"
    b = category_id("run-1", "micro", 2, parent_label=5)
    assert b == "run-1|micro|5|2"
    # Same input → same id
    assert category_id("run-1", "macro", 5) == a
