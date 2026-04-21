"""Round-trip tests for JsonFileReader against JsonFileWriter output."""

from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.readers import JsonFileReader, ProcessoTimeline, ReaderError
from pipeline.writers import JsonFileWriter


# Representative transform_row-shaped activity rows as they appear inside
# the LOAD_ATIVIDADES_CYPHER template params.
def _sample_rows() -> list[dict]:
    return [
        # Processo P1, three activities out of chronological order
        {"source_id": "P1-3", "protocolo_formatado": "P1",
         "data_hora": "2026-01-03T09:00:00", "unidade": "U1",
         "tipo_acao": "PROCESSO-RECEBIDO-UNIDADE", "grupo": "g",
         "ref_id": None, "seq": 3, "source_unidade": None},
        {"source_id": "P1-1", "protocolo_formatado": "P1",
         "data_hora": "2026-01-01T08:00:00", "unidade": "U1",
         "tipo_acao": "GERACAO-PROCEDIMENTO", "grupo": "g",
         "ref_id": None, "seq": 1, "source_unidade": None},
        {"source_id": "P1-2", "protocolo_formatado": "P1",
         "data_hora": "2026-01-02T08:00:00", "unidade": "U2",
         "tipo_acao": "PROCESSO-REMETIDO-UNIDADE", "grupo": "g",
         "ref_id": None, "seq": 2, "source_unidade": "U1"},
        # Processo P2
        {"source_id": "P2-1", "protocolo_formatado": "P2",
         "data_hora": "2026-02-01T12:00:00", "unidade": "U3",
         "tipo_acao": "GERACAO-PROCEDIMENTO", "grupo": "g",
         "ref_id": None, "seq": 1, "source_unidade": None},
    ]


def test_round_trip_load_atividades(tmp_path: Path) -> None:
    # Arrange — write via JsonFileWriter as Phase B does
    w = JsonFileWriter(tmp_path)
    w.open_phase("B")
    w.execute_template("load_atividades", "UNWIND $rows ...",
                       {"rows": _sample_rows()}, phase="B")
    w.close_phase("B")
    w.close()

    # Act
    r = JsonFileReader(tmp_path)
    assert r.count_processos() == 2
    batches = list(r.iter_processo_batches(batch_size=10))
    r.close()

    # Assert — two processos, sorted by protocolo, activities sorted by (data_hora, source_id)
    assert len(batches) == 1
    batch = batches[0]
    assert [p.protocolo_formatado for p in batch] == ["P1", "P2"]

    p1, p2 = batch
    assert [a["source_id"] for a in p1.activities] == ["P1-1", "P1-2", "P1-3"]
    assert p1.activities[1]["source_unidade"] == "U1"  # the REMETIDO one
    assert p1.activities[0]["source_unidade"] is None
    assert [a["unidade"] for a in p1.activities] == ["U1", "U2", "U1"]

    assert len(p2.activities) == 1
    assert p2.activities[0]["source_id"] == "P2-1"


def test_reader_handles_multiple_template_batches(tmp_path: Path) -> None:
    """Phase B workers each write their own template line; the reader must
    merge them back."""
    rows_all = _sample_rows()
    w = JsonFileWriter(tmp_path)
    w.execute_template("load_atividades", "...", {"rows": rows_all[:2]}, phase="B")
    w.execute_template("load_atividades", "...", {"rows": rows_all[2:]}, phase="B")
    w.close()

    r = JsonFileReader(tmp_path)
    timelines = list(r.iter_processo_batches(batch_size=10))[0]
    assert len(timelines) == 2
    assert sum(len(t.activities) for t in timelines) == 4


def test_batch_size_paginates(tmp_path: Path) -> None:
    # Build rows for 5 distinct processos
    rows = []
    for i in range(5):
        rows.append({
            "source_id": f"P{i}-1", "protocolo_formatado": f"P{i:02d}",
            "data_hora": "2026-01-01T00:00:00", "unidade": "U",
            "tipo_acao": "X", "grupo": "g",
            "ref_id": None, "seq": 1, "source_unidade": None,
        })
    w = JsonFileWriter(tmp_path)
    w.execute_template("load_atividades", "...", {"rows": rows}, phase="B")
    w.close()

    r = JsonFileReader(tmp_path)
    batches = list(r.iter_processo_batches(batch_size=2))
    assert [len(b) for b in batches] == [2, 2, 1]
    assert [p.protocolo_formatado for b in batches for p in b] == [
        "P00", "P01", "P02", "P03", "P04"
    ]


def test_reader_raises_when_template_missing(tmp_path: Path) -> None:
    # Empty dir — no templates subdirectory at all
    (tmp_path / "nodes").mkdir()
    (tmp_path / "edges").mkdir()
    (tmp_path / "templates").mkdir()
    r = JsonFileReader(tmp_path)
    with pytest.raises(ReaderError, match="Phase B template output is required"):
        r.count_processos()


def test_reader_requires_directory(tmp_path: Path) -> None:
    missing = tmp_path / "does_not_exist"
    with pytest.raises(ReaderError, match="is not a directory"):
        JsonFileReader(missing)


def test_empty_template_file_yields_zero_processos(tmp_path: Path) -> None:
    (tmp_path / "templates").mkdir()
    (tmp_path / "templates" / "load_atividades.ndjson").write_text("")
    r = JsonFileReader(tmp_path)
    assert r.count_processos() == 0
    assert list(r.iter_processo_batches()) == []
