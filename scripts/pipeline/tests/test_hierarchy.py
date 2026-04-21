from __future__ import annotations

from pipeline.hierarchy import all_ancestor_unidades, parent_unidade


def test_parent_unidade_depth_three() -> None:
    assert parent_unidade("SEAD-PI/GAB/NTGD") == "SEAD-PI/GAB"


def test_parent_unidade_depth_two() -> None:
    assert parent_unidade("SEAD-PI/GAB") == "SEAD-PI"


def test_parent_unidade_root_is_none() -> None:
    assert parent_unidade("SEAD-PI") is None


def test_all_ancestors_deep() -> None:
    assert all_ancestor_unidades("A/B/C/D") == ["A/B/C", "A/B", "A"]


def test_all_ancestors_shallow() -> None:
    assert all_ancestor_unidades("A/B") == ["A"]


def test_all_ancestors_root_is_empty() -> None:
    assert all_ancestor_unidades("A") == []
