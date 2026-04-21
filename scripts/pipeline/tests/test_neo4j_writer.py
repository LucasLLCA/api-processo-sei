"""Unit tests for DirectNeo4jWriter Cypher generation and dispatch.

Uses a fake driver so no live Neo4j instance is required.
"""

from __future__ import annotations

from typing import Any

from pipeline.writers import DirectNeo4jWriter, WriterError
import pytest


class _RecordingSession:
    def __init__(self, driver: "_RecordingDriver") -> None:
        self._driver = driver

    def __enter__(self) -> "_RecordingSession":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def run(self, cypher: str, **params: Any) -> None:
        self._driver.calls.append((cypher, params))


class _RecordingDriver:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def session(self) -> _RecordingSession:
        return _RecordingSession(self)


# -- write_nodes ------------------------------------------------------------

def test_write_nodes_minimal_merge() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d, batch_size=1000)
    count = w.write_nodes("Orgao", ["sigla"], [{"sigla": "SEAD-PI"}], phase="A")
    assert count == 1
    assert len(d.calls) == 1
    cypher, params = d.calls[0]
    assert "MERGE (n:Orgao {sigla: r.sigla})" in cypher
    assert "UNWIND $rows AS r" in cypher
    assert "SET" not in cypher
    assert params == {"rows": [{"sigla": "SEAD-PI"}]}


def test_write_nodes_with_props() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d)
    rows = [{"sigla": "U1", "id_unidade": 1, "descricao": "Foo"}]
    w.write_nodes("Unidade", ["sigla"], rows, phase="A", props=["id_unidade", "descricao"])
    cypher, _ = d.calls[0]
    assert "MERGE (n:Unidade {sigla: r.sigla})" in cypher
    assert "SET n.id_unidade = r.id_unidade, n.descricao = r.descricao" in cypher


def test_write_nodes_infers_props_when_none() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d)
    w.write_nodes("GrupoAtividade", ["chave"],
                  [{"chave": "X", "label": "L", "horas": 2}], phase="A")
    cypher, _ = d.calls[0]
    assert "n.label = r.label" in cypher
    assert "n.horas = r.horas" in cypher
    assert "n.chave" not in cypher  # key isn't SET


def test_write_nodes_empty_rows_is_noop() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d)
    assert w.write_nodes("Orgao", ["sigla"], [], phase="A") == 0
    assert d.calls == []


def test_write_nodes_requires_key_fields() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d)
    with pytest.raises(WriterError, match="key field"):
        w.write_nodes("Orgao", [], [{"sigla": "X"}], phase="A")


def test_write_nodes_batches() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d, batch_size=2)
    rows = [{"sigla": f"U{i}"} for i in range(5)]
    w.write_nodes("Unidade", ["sigla"], rows, phase="A")
    assert len(d.calls) == 3  # 2 + 2 + 1
    assert d.calls[0][1]["rows"] == rows[0:2]
    assert d.calls[2][1]["rows"] == rows[4:5]


# -- write_edges ------------------------------------------------------------

def test_write_edges_simple() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d)
    rows = [{"unidade": "U1", "orgao": "O1"}]
    w.write_edges(
        "PERTENCE_AO_ORGAO",
        "Unidade", {"unidade": "sigla"},
        "Orgao", {"orgao": "sigla"},
        rows,
        phase="A",
    )
    cypher, _ = d.calls[0]
    assert "MATCH (a:Unidade {sigla: r.unidade})" in cypher
    assert "MATCH (b:Orgao {sigla: r.orgao})" in cypher
    assert "MERGE (a)-[rel:PERTENCE_AO_ORGAO]->(b)" in cypher


def test_write_edges_same_label_disambiguates_via_row_fields() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d)
    rows = [{"child": "U1/SUB", "parent": "U1"}]
    w.write_edges(
        "SUBUNIDADE_DE",
        "Unidade", {"child": "sigla"},
        "Unidade", {"parent": "sigla"},
        rows,
        phase="A",
    )
    cypher, _ = d.calls[0]
    assert "MATCH (a:Unidade {sigla: r.child})" in cypher
    assert "MATCH (b:Unidade {sigla: r.parent})" in cypher


def test_write_edges_with_props() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d)
    rows = [{"from_key": "A", "to_key": "B", "weight": 0.9}]
    w.write_edges(
        "LINKS",
        "X", {"from_key": "id"},
        "Y", {"to_key": "id"},
        rows,
        phase="C",
        props=["weight"],
    )
    cypher, _ = d.calls[0]
    assert "SET rel.weight = r.weight" in cypher


def test_write_edges_empty_rows_is_noop() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d)
    assert w.write_edges("R", "A", {"k": "k"}, "B", {"k": "k"}, [], phase="A") == 0
    assert d.calls == []


# -- execute_template --------------------------------------------------------

def test_execute_template_passes_through() -> None:
    d = _RecordingDriver()
    w = DirectNeo4jWriter(d)
    cypher = "UNWIND $foo AS x MERGE (:Bar {k: x.k})"
    w.execute_template("bar_seed", cypher, {"foo": [{"k": 1}]}, phase="A")
    assert d.calls == [(cypher, {"foo": [{"k": 1}]})]
