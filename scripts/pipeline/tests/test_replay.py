"""End-to-end replay tests.

Strategy: use JsonFileWriter to emit a synthetic graph, then have replay.py
walk the emit dir and route writes through a fake recording driver. Verify
the recorded Cypher and params are what we'd expect.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pipeline.replay import replay_emit_dir
from pipeline.writers import DirectNeo4jWriter, JsonFileWriter


# -- fake driver ------------------------------------------------------------

class _FakeSession:
    def __init__(self, driver: "_FakeDriver") -> None:
        self._driver = driver

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def run(self, cypher: str, **params: Any) -> None:
        self._driver.calls.append((cypher, params))


class _FakeDriver:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def session(self) -> _FakeSession:
        return _FakeSession(self)


def _new_writer() -> tuple[_FakeDriver, DirectNeo4jWriter]:
    driver = _FakeDriver()
    return driver, DirectNeo4jWriter(driver, batch_size=1000)


def _emit_phase_a_sample(emit_dir: Path) -> None:
    """Populate an emit directory with a small Phase A sample."""
    jw = JsonFileWriter(emit_dir)
    jw.open_phase("A")

    jw.write_nodes("Orgao", ["sigla"],
                   [{"sigla": "SEAD-PI"}, {"sigla": "SEFAZ-PI"}], phase="A")

    jw.write_nodes("Unidade", ["sigla"], [
        {"sigla": "SEAD-PI/GAB", "id_unidade": 1, "descricao": "Gabinete"},
        {"sigla": "SEFAZ-PI/DIR", "id_unidade": 2, "descricao": "Diretoria"},
    ], phase="A", props=["id_unidade", "descricao"])

    jw.write_edges(
        "PERTENCE_AO_ORGAO",
        "Unidade", {"unidade": "sigla"},
        "Orgao", {"orgao": "sigla"},
        [
            {"unidade": "SEAD-PI/GAB", "orgao": "SEAD-PI"},
            {"unidade": "SEFAZ-PI/DIR", "orgao": "SEFAZ-PI"},
        ],
        phase="A",
    )

    jw.execute_template(
        "seed_tipos",
        "UNWIND $types AS t MERGE (:TipoAcao {chave: t.chave})",
        {"types": [{"chave": "X"}, {"chave": "Y"}]},
        phase="A",
    )
    jw.close_phase("A")
    jw.close()


# -- tests -----------------------------------------------------------------

def test_replay_walks_nodes_templates_edges_in_order(tmp_path: Path) -> None:
    _emit_phase_a_sample(tmp_path)

    driver, writer = _new_writer()
    counts = replay_emit_dir(writer, tmp_path, ["A"])
    writer.close()

    assert counts == {"nodes": 4, "edges": 2, "templates": 1}

    # Order check: every MERGE (:Orgao / :Unidade) should precede every
    # MATCH that references them.
    cyphers = [c for c, _p in driver.calls]
    orgao_idx = next(i for i, c in enumerate(cyphers) if "MERGE (n:Orgao" in c)
    pertence_idx = next(i for i, c in enumerate(cyphers)
                        if "MERGE (a)-[rel:PERTENCE_AO_ORGAO]" in c)
    assert orgao_idx < pertence_idx

    # Template goes through unchanged
    seed_calls = [(c, p) for c, p in driver.calls if "TipoAcao" in c]
    assert len(seed_calls) == 1
    cypher, params = seed_calls[0]
    assert params == {"types": [{"chave": "X"}, {"chave": "Y"}]}


def test_replay_write_nodes_includes_props(tmp_path: Path) -> None:
    _emit_phase_a_sample(tmp_path)
    driver, writer = _new_writer()
    replay_emit_dir(writer, tmp_path, ["A"])
    writer.close()

    # Find the Unidade MERGE call and make sure props came through
    unidade = next((c, p) for c, p in driver.calls if "MERGE (n:Unidade" in c)
    cypher, params = unidade
    assert "n.id_unidade = r.id_unidade" in cypher
    assert "n.descricao = r.descricao" in cypher
    rows = params["rows"]
    assert {r["sigla"] for r in rows} == {"SEAD-PI/GAB", "SEFAZ-PI/DIR"}
    assert rows[0]["id_unidade"] in (1, 2)


def test_replay_write_edges_preserves_endpoints(tmp_path: Path) -> None:
    _emit_phase_a_sample(tmp_path)
    driver, writer = _new_writer()
    replay_emit_dir(writer, tmp_path, ["A"])
    writer.close()

    edge_call = next((c, p) for c, p in driver.calls
                     if "MERGE (a)-[rel:PERTENCE_AO_ORGAO]" in c)
    cypher, params = edge_call
    assert "MATCH (a:Unidade {sigla: r.from__sigla})" in cypher
    assert "MATCH (b:Orgao {sigla: r.to__sigla})" in cypher
    rows = params["rows"]
    siglas_from = {r["from__sigla"] for r in rows}
    siglas_to = {r["to__sigla"] for r in rows}
    assert siglas_from == {"SEAD-PI/GAB", "SEFAZ-PI/DIR"}
    assert siglas_to == {"SEAD-PI", "SEFAZ-PI"}


def test_replay_phase_filter(tmp_path: Path) -> None:
    jw = JsonFileWriter(tmp_path)
    jw.write_nodes("Orgao", ["sigla"], [{"sigla": "A"}], phase="A")
    jw.write_nodes("Orgao", ["sigla"], [{"sigla": "B"}], phase="B")
    jw.close()

    driver, writer = _new_writer()
    counts = replay_emit_dir(writer, tmp_path, ["A"])
    writer.close()

    assert counts["nodes"] == 1
    orgao_merges = [(c, p) for c, p in driver.calls if "MERGE (n:Orgao" in c]
    assert len(orgao_merges) == 1
    assert orgao_merges[0][1]["rows"] == [{"sigla": "A"}]


def test_replay_empty_dir_is_noop(tmp_path: Path) -> None:
    # Empty emit dir (no NDJSON shards at all)
    (tmp_path / "nodes").mkdir()
    (tmp_path / "edges").mkdir()
    (tmp_path / "templates").mkdir()

    driver, writer = _new_writer()
    counts = replay_emit_dir(writer, tmp_path, ["A", "B", "C", "D"])
    writer.close()

    assert counts == {"nodes": 0, "edges": 0, "templates": 0}
    assert driver.calls == []


def test_replay_round_trip_count_matches_emitted(tmp_path: Path) -> None:
    """Emit 5 nodes + 3 edges + 2 templates, replay, check totals."""
    jw = JsonFileWriter(tmp_path)
    jw.write_nodes("Orgao", ["sigla"],
                   [{"sigla": f"O{i}"} for i in range(5)], phase="A")
    jw.write_edges(
        "LINKS",
        "Orgao", {"a": "sigla"},
        "Orgao", {"b": "sigla"},
        [{"a": "O0", "b": "O1"},
         {"a": "O1", "b": "O2"},
         {"a": "O2", "b": "O3"}],
        phase="A",
    )
    jw.execute_template("t1", "RETURN 1", {}, phase="A")
    jw.execute_template("t2", "RETURN 2", {}, phase="A")
    jw.close()

    driver, writer = _new_writer()
    counts = replay_emit_dir(writer, tmp_path, ["A"])
    writer.close()

    assert counts == {"nodes": 5, "edges": 3, "templates": 2}
