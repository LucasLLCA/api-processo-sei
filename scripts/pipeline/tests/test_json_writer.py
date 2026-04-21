"""Unit tests for JsonFileWriter — NDJSON emission, manifest, round-trip."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.writers import JsonFileWriter, WriterError
from pipeline.writers.json_writer import _to_shard_name


# -- helpers ---------------------------------------------------------------

def _read_lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


# -- filename conversion ---------------------------------------------------

def test_to_shard_name_camel() -> None:
    assert _to_shard_name("GrupoAtividade") == "grupo_atividade"
    assert _to_shard_name("Unidade") == "unidade"


def test_to_shard_name_screaming_snake() -> None:
    assert _to_shard_name("PERTENCE_AO_ORGAO") == "pertence_ao_orgao"
    assert _to_shard_name("SEGUIDO_INDEPENDENTEMENTE_POR") == "seguido_independentemente_por"


# -- layout & manifest -----------------------------------------------------

def test_output_dir_layout_created(tmp_path: Path) -> None:
    out = tmp_path / "emit"
    JsonFileWriter(out).close()
    assert (out / "nodes").is_dir()
    assert (out / "edges").is_dir()
    assert (out / "templates").is_dir()
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["schema_version"] == 1
    assert manifest["started_at"] is not None
    assert manifest["completed_at"] is not None


def test_manifest_tracks_phase_progress(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    w.open_phase("A")
    w.write_nodes("Orgao", ["sigla"], [{"sigla": "X"}], phase="A")
    mid = json.loads((tmp_path / "manifest.json").read_text())
    assert mid["phases_in_progress"] == ["A"]
    assert mid["phases_completed"] == []
    w.close_phase("A")
    after = json.loads((tmp_path / "manifest.json").read_text())
    assert after["phases_in_progress"] == []
    assert after["phases_completed"] == ["A"]
    w.close()


# -- write_nodes -----------------------------------------------------------

def test_write_nodes_emits_one_line_per_row(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    rows = [
        {"sigla": "U1", "id_unidade": 1, "descricao": "Foo"},
        {"sigla": "U2", "id_unidade": 2, "descricao": "Bar"},
    ]
    w.write_nodes("Unidade", ["sigla"], rows, phase="A",
                  props=["id_unidade", "descricao"])
    w.close()

    lines = _read_lines(tmp_path / "nodes" / "unidade.ndjson")
    assert len(lines) == 2
    assert lines[0] == {
        "label": "Unidade",
        "key": {"sigla": "U1"},
        "props": {"id_unidade": 1, "descricao": "Foo"},
        "phase": "A",
    }
    assert lines[1]["key"] == {"sigla": "U2"}


def test_write_nodes_infers_props(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    w.write_nodes(
        "GrupoAtividade", ["chave"],
        [{"chave": "k", "label": "L", "horas": 2}],
        phase="A",
    )
    w.close()
    line = _read_lines(tmp_path / "nodes" / "grupo_atividade.ndjson")[0]
    assert line["key"] == {"chave": "k"}
    assert line["props"] == {"label": "L", "horas": 2}


def test_write_nodes_empty_is_noop(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    assert w.write_nodes("Orgao", ["sigla"], [], phase="A") == 0
    w.close()
    assert not (tmp_path / "nodes" / "orgao.ndjson").exists()


def test_write_nodes_requires_key_fields(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    with pytest.raises(WriterError, match="key field"):
        w.write_nodes("Orgao", [], [{"sigla": "X"}], phase="A")
    w.close()


def test_write_nodes_updates_counts(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    w.write_nodes("Orgao", ["sigla"], [{"sigla": "A"}, {"sigla": "B"}], phase="A")
    w.write_nodes("Orgao", ["sigla"], [{"sigla": "C"}], phase="A")
    w.close()
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["nodes_counts"] == {"Orgao": 3}


# -- write_edges -----------------------------------------------------------

def test_write_edges_shape(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    w.write_edges(
        "PERTENCE_AO_ORGAO",
        "Unidade", {"unidade": "sigla"},
        "Orgao", {"orgao": "sigla"},
        [{"unidade": "U1", "orgao": "O1"}],
        phase="A",
    )
    w.close()
    line = _read_lines(tmp_path / "edges" / "pertence_ao_orgao.ndjson")[0]
    assert line == {
        "type": "PERTENCE_AO_ORGAO",
        "from": {"label": "Unidade", "key": {"sigla": "U1"}},
        "to":   {"label": "Orgao", "key": {"sigla": "O1"}},
        "props": {},
        "phase": "A",
    }


def test_write_edges_same_label_disambiguates(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    w.write_edges(
        "SUBUNIDADE_DE",
        "Unidade", {"child": "sigla"},
        "Unidade", {"parent": "sigla"},
        [{"child": "A/B", "parent": "A"}],
        phase="A",
    )
    w.close()
    line = _read_lines(tmp_path / "edges" / "subunidade_de.ndjson")[0]
    assert line["from"]["key"] == {"sigla": "A/B"}
    assert line["to"]["key"] == {"sigla": "A"}


def test_write_edges_with_props(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    w.write_edges(
        "SEGUIDO_INDEPENDENTEMENTE_POR",
        "Atividade", {"from_id": "source_id"},
        "Atividade", {"to_id": "source_id"},
        [{"from_id": "1", "to_id": "2", "ref_id": "doc:42"}],
        phase="C",
        props=["ref_id"],
    )
    w.close()
    line = _read_lines(tmp_path / "edges" / "seguido_independentemente_por.ndjson")[0]
    assert line["props"] == {"ref_id": "doc:42"}


# -- execute_template ------------------------------------------------------

def test_execute_template_emits_raw_cypher(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    cypher = "UNWIND $types AS t MERGE (:TipoAcao {chave: t.chave})"
    w.execute_template("seed_tipos", cypher, {"types": [{"chave": "A"}]}, phase="A")
    w.close()
    line = _read_lines(tmp_path / "templates" / "seed_tipos.ndjson")[0]
    assert line == {
        "name": "seed_tipos",
        "cypher": cypher,
        "params": {"types": [{"chave": "A"}]},
        "phase": "A",
    }


def test_execute_template_counts(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    for _ in range(3):
        w.execute_template("schema_constraint", "CREATE CONSTRAINT x", {}, phase="schema")
    w.close()
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["template_counts"] == {"schema_constraint": 3}


# -- round-trip ------------------------------------------------------------

def test_round_trip_preserves_rows(tmp_path: Path) -> None:
    w = JsonFileWriter(tmp_path)
    w.open_phase("A")
    orgao_rows = [{"sigla": "SEAD-PI"}, {"sigla": "SEFAZ-PI"}]
    unidade_rows = [
        {"sigla": "SEAD-PI/GAB", "id_unidade": 1, "descricao": "Gabinete"},
        {"sigla": "SEFAZ-PI/DIR", "id_unidade": 2, "descricao": "Diretoria"},
    ]
    edge_rows = [
        {"unidade": "SEAD-PI/GAB", "orgao": "SEAD-PI"},
        {"unidade": "SEFAZ-PI/DIR", "orgao": "SEFAZ-PI"},
    ]
    w.write_nodes("Orgao", ["sigla"], orgao_rows, phase="A")
    w.write_nodes("Unidade", ["sigla"], unidade_rows, phase="A",
                  props=["id_unidade", "descricao"])
    w.write_edges(
        "PERTENCE_AO_ORGAO",
        "Unidade", {"unidade": "sigla"},
        "Orgao", {"orgao": "sigla"},
        edge_rows, phase="A",
    )
    w.close_phase("A")
    w.close()

    # Read back each shard and verify counts
    orgaos = _read_lines(tmp_path / "nodes" / "orgao.ndjson")
    unidades = _read_lines(tmp_path / "nodes" / "unidade.ndjson")
    edges = _read_lines(tmp_path / "edges" / "pertence_ao_orgao.ndjson")
    assert len(orgaos) == 2
    assert len(unidades) == 2
    assert len(edges) == 2
    assert {o["key"]["sigla"] for o in orgaos} == {"SEAD-PI", "SEFAZ-PI"}
    # Manifest counts match
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["nodes_counts"] == {"Orgao": 2, "Unidade": 2}
    assert manifest["edges_counts"] == {"PERTENCE_AO_ORGAO": 2}
    assert manifest["phases_completed"] == ["A"]


# -- context manager -------------------------------------------------------

def test_context_manager_closes(tmp_path: Path) -> None:
    with JsonFileWriter(tmp_path) as w:
        w.write_nodes("Orgao", ["sigla"], [{"sigla": "X"}], phase="A")
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["completed_at"] is not None
