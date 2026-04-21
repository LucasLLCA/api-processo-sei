from __future__ import annotations

from pipeline.classification import extract_orgao


def test_extract_orgao_deep() -> None:
    assert extract_orgao("SEAD-PI/GAB/SGACG") == "SEAD-PI"


def test_extract_orgao_shallow() -> None:
    assert extract_orgao("SEAD-PI") == "SEAD-PI"


def test_extract_orgao_empty_returns_desconhecido() -> None:
    assert extract_orgao("") == "DESCONHECIDO"


def test_extract_orgao_none_returns_desconhecido() -> None:
    assert extract_orgao(None) == "DESCONHECIDO"


def test_extract_orgao_single_slash() -> None:
    assert extract_orgao("A/B") == "A"
