"""Neo4jReader — reads processo timelines from a live Neo4j driver.

Preserves the exact Cypher previously inlined in `etl_neo4j.build_timeline`
(Phase C). Phase D's query was a strict subset of the same data, so both
phases now consume this single reader.
"""

from __future__ import annotations

import logging
from typing import Any, Iterator

from .base import GraphReader, ProcessoTimeline

log = logging.getLogger(__name__)

_COUNT_PROCESSOS_CYPHER = "MATCH (p:Processo) RETURN count(p) AS cnt"

# Preserved verbatim from etl_neo4j.build_timeline. Returns one row per
# processo with its activities collected and sorted by data_hora, source_id.
_BATCH_PROCESSOS_CYPHER = """
MATCH (p:Processo)
WITH p ORDER BY p.protocolo_formatado SKIP $skip LIMIT $limit
MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
MATCH (a)-[:EXECUTADO_PELA_UNIDADE]->(u:Unidade)
OPTIONAL MATCH (a)-[:REMETIDO_PELA_UNIDADE]->(src:Unidade)
WITH p.protocolo_formatado AS pf,
     a.source_id AS source_id,
     a.data_hora AS data_hora,
     a.tipo_acao AS tipo_acao,
     a.ref_id AS ref_id,
     u.sigla AS unidade,
     src.sigla AS source_unidade
ORDER BY pf, data_hora, source_id
RETURN pf, collect({
    source_id: source_id,
    data_hora: toString(data_hora),
    tipo_acao: tipo_acao,
    ref_id: ref_id,
    unidade: unidade,
    source_unidade: source_unidade
}) AS activities
"""


class Neo4jReader(GraphReader):
    def __init__(self, driver: Any) -> None:
        self._driver = driver

    def close(self) -> None:
        # Caller owns driver lifetime; do not close here.
        pass

    def count_processos(self) -> int:
        with self._driver.session() as session:
            record = session.run(_COUNT_PROCESSOS_CYPHER).single()
        return int(record["cnt"]) if record else 0

    def iter_processo_batches(
        self, *, batch_size: int = 500,
    ) -> Iterator[list[ProcessoTimeline]]:
        skip = 0
        total = self.count_processos()
        while skip < total:
            with self._driver.session() as session:
                result = session.run(
                    _BATCH_PROCESSOS_CYPHER, skip=skip, limit=batch_size,
                )
                batch: list[ProcessoTimeline] = [
                    ProcessoTimeline(
                        protocolo_formatado=record["pf"],
                        activities=list(record["activities"] or []),
                    )
                    for record in result
                ]
            yield batch
            skip += batch_size
