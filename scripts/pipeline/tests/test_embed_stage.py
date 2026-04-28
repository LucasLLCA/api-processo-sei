"""Tests for ``pipeline.stages.embed``: hash invalidation + iter parsed docs."""

from __future__ import annotations

from pathlib import Path

from pipeline.stages.embed import _hash_for, _iter_parsed_docs


def test_hash_for_deterministic():
    h1 = _hash_for("texto", "BAAI/bge-m3")
    h2 = _hash_for("texto", "BAAI/bge-m3")
    assert h1 == h2 and len(h1) == 64


def test_hash_for_changes_when_text_changes():
    a = _hash_for("a", "m")
    b = _hash_for("b", "m")
    assert a != b


def test_hash_for_changes_when_model_changes():
    a = _hash_for("a", "m1")
    b = _hash_for("a", "m2")
    assert a != b


def test_iter_parsed_docs_skips_empty(tmp_path: Path):
    (tmp_path / "doc1.txt").write_text("hello", encoding="utf-8")
    (tmp_path / "empty.txt").write_text("   \n  ", encoding="utf-8")
    (tmp_path / "doc2.txt").write_text("world", encoding="utf-8")
    out = list(_iter_parsed_docs(tmp_path))
    assert len(out) == 2
    numeros = {n for n, _ in out}
    assert numeros == {"doc1", "doc2"}


def test_iter_parsed_docs_yields_text_content(tmp_path: Path):
    (tmp_path / "x.txt").write_text("Olá mundo SEI", encoding="utf-8")
    out = dict(_iter_parsed_docs(tmp_path))
    assert out["x"] == "Olá mundo SEI"


def test_iter_parsed_docs_ignores_non_txt(tmp_path: Path):
    (tmp_path / "x.txt").write_text("yes", encoding="utf-8")
    (tmp_path / "x.json").write_text("no", encoding="utf-8")
    out = list(_iter_parsed_docs(tmp_path))
    assert len(out) == 1
