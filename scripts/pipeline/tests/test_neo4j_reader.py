"""Unit tests for Neo4jReader using a fake driver.

Verifies:
- count_processos issues the right Cypher and returns the count
- iter_processo_batches paginates with correct SKIP/LIMIT and yields
  ProcessoTimeline objects with activities as list[dict]
"""

from __future__ import annotations

from typing import Any

from pipeline.readers import Neo4jReader, ProcessoTimeline


class _FakeRecord:
    def __init__(self, data: dict) -> None:
        self._data = data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]


class _FakeResult:
    def __init__(self, records: list[_FakeRecord]) -> None:
        self._records = records

    def __iter__(self):
        return iter(self._records)

    def single(self) -> _FakeRecord | None:
        return self._records[0] if self._records else None


class _FakeSession:
    def __init__(self, driver: "_FakeDriver") -> None:
        self._driver = driver

    def __enter__(self) -> "_FakeSession":
        return self

    def __exit__(self, *_exc: Any) -> None:
        return None

    def run(self, cypher: str, **params: Any) -> _FakeResult:
        self._driver.calls.append((cypher, params))
        return self._driver.next_result()


class _FakeDriver:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._results: list[_FakeResult] = []

    def session(self) -> _FakeSession:
        return _FakeSession(self)

    def queue(self, records: list[dict]) -> None:
        self._results.append(_FakeResult([_FakeRecord(r) for r in records]))

    def next_result(self) -> _FakeResult:
        return self._results.pop(0)


def test_count_processos() -> None:
    d = _FakeDriver()
    d.queue([{"cnt": 42}])
    r = Neo4jReader(d)
    assert r.count_processos() == 42
    assert "count(p)" in d.calls[0][0]


def test_iter_processo_batches_paginates() -> None:
    d = _FakeDriver()
    # count result
    d.queue([{"cnt": 3}])
    # first batch (skip=0, limit=2)
    d.queue([
        {"pf": "A1", "activities": [
            {"source_id": "1", "data_hora": "2026-01-01T00:00:00",
             "tipo_acao": "X", "ref_id": None,
             "unidade": "U1", "source_unidade": None},
        ]},
        {"pf": "A2", "activities": []},
    ])
    # second batch (skip=2, limit=2) — last processo
    d.queue([
        {"pf": "A3", "activities": [
            {"source_id": "2", "data_hora": "2026-01-02T00:00:00",
             "tipo_acao": "Y", "ref_id": "doc:7",
             "unidade": "U2", "source_unidade": "U1"},
        ]},
    ])

    r = Neo4jReader(d)
    batches = list(r.iter_processo_batches(batch_size=2))

    assert len(batches) == 2
    assert len(batches[0]) == 2
    assert isinstance(batches[0][0], ProcessoTimeline)
    assert batches[0][0].protocolo_formatado == "A1"
    assert batches[0][0].activities[0]["source_id"] == "1"
    assert batches[0][1].protocolo_formatado == "A2"
    assert batches[0][1].activities == []
    assert batches[1][0].protocolo_formatado == "A3"
    assert batches[1][0].activities[0]["ref_id"] == "doc:7"

    # Verify pagination parameters
    # calls[0] = count, calls[1] = skip=0 limit=2, calls[2] = skip=2 limit=2
    assert d.calls[1][1] == {"skip": 0, "limit": 2}
    assert d.calls[2][1] == {"skip": 2, "limit": 2}


def test_iter_empty_when_no_processos() -> None:
    d = _FakeDriver()
    d.queue([{"cnt": 0}])
    r = Neo4jReader(d)
    batches = list(r.iter_processo_batches(batch_size=100))
    assert batches == []
