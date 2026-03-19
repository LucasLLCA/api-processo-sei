"""
ETL script: PostgreSQL sei_atividades → Neo4j graph.

Graph model (enhanced for process flow clustering, shortest path & permanência):

  Nodes:
    - Processo         {protocolo_formatado*, data_criacao}
    - Atividade        {source_id*, data_hora, descricao, tipo_acao, grupo, ref_id, seq}
    - TipoProcedimento {nome*}
    - Unidade          {sigla*}
    - Orgao            {sigla*}
    - Usuario          {nome*}
    - TipoAcao         {chave*}
    - GrupoAtividade   {chave*, label, horas}
    - Documento        {numero*, tipo, serie_id}

  Relationships:
    - (Atividade)-[:DO_PROCESSO]->(Processo)
    - (Atividade)-[:EXECUTADO_PELA_UNIDADE]->(Unidade)
    - (Atividade)-[:TIPO_ACAO]->(TipoAcao)
    - (Atividade)-[:EXECUTADO_PELO_USUARIO]->(Usuario)
    - (Atividade)-[:REMETIDO_PELA_UNIDADE]->(Unidade)       # source unit on remetido
    - (Atividade)-[:REFERENCIA_DOCUMENTO]->(Documento)       # document referenced in activity
    - (TipoAcao)-[:PERTENCE_AO_GRUPO]->(GrupoAtividade)
    - (Processo)-[:TEM_TIPO]->(TipoProcedimento)
    - (Processo)-[:CRIADO_NA_UNIDADE]->(Unidade)
    - (Processo)-[:CRIADO_NO_ORGAO]->(Orgao)
    - (Processo)-[:PASSOU_PELA_UNIDADE {duracao_total_horas, visitas, ...}]->(Unidade)
    - (Processo)-[:PASSOU_PELO_ORGAO {duracao_total_horas, visitas, ...}]->(Orgao)
    - (Unidade)-[:PERTENCE_AO_ORGAO]->(Orgao)
    - (Unidade)-[:SUBUNIDADE_DE]->(Unidade)                  # SEAD-PI/GAB/NTGD → SEAD-PI/GAB
    - (Usuario)-[:PERTENCE_AO_ORGAO]->(Orgao)
    - (Processo)-[:INICIOU_PROCESSO]->(Atividade)              # first activity
    - (Atividade)-[:SEGUIDA_POR {mesma_unidade}]->(Atividade)  # DAG timeline forward
    - (Atividade)-[:PRECEDIDA_POR {mesma_unidade}]->(Atividade) # DAG timeline backward
    - (Atividade)-[:SEGUIDO_INDEPENDENTEMENTE_POR {ref_id}]->(Atividade) # non-flow link via bloco/doc ref

Strategy to avoid deadlocks:
    Phase A: Pre-create shared dimension nodes (single-threaded)
    Phase B: Load atividades + per-atividade relationships (parallel)
    Phase C: Build timeline DAG SEGUIDA_POR (batched)
    Phase D: Compute permanencia PASSOU_PELA_UNIDADE + PASSOU_PELO_ORGAO (batched)

Usage:
    python scripts/etl_neo4j.py --from 2025-01-01 --to 2026-01-01
    python scripts/etl_neo4j.py --dry-run
    python scripts/etl_neo4j.py --workers 10 --batch-size 500
"""

import argparse
import logging
import random
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
from neo4j import GraphDatabase
from neo4j.exceptions import TransientError

from etl_neo4j_classifier import (
    classify_descricao,
    extract_document_info,
    extract_orgao,
    extract_reference_id,
    extract_source_unidade,
    get_all_grupo_records,
    get_all_tipo_acao_records,
    get_grupo,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PG_HOST = "db-sead-intsei-prod.cpqw468qwjp2.sa-east-1.rds.amazonaws.com"
PG_PORT = 5432
PG_USER = "gabriel_coelho"
PG_PASSWORD = "123456"
PG_DATABASE = "sead"

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"

TZ = ZoneInfo("America/Fortaleza")

MAX_RETRIES = 20

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------
FIND_PROCESSOS_SQL = """
    SELECT protocolo_formatado, data_hora, unidade, usuario, tipo_procedimento
    FROM sei_processo.sei_atividades
    WHERE descricao_replace LIKE 'Processo %%gerado%%'
    {date_filter}
    ORDER BY data_hora
"""

FETCH_ANDAMENTOS_SQL = """
    SELECT id, protocolo_formatado, data_hora,
           unidade, usuario, tipo_procedimento, descricao_replace
    FROM sei_processo.sei_atividades
    WHERE protocolo_formatado = ANY(%s)
    ORDER BY protocolo_formatado, data_hora
"""

FIND_USUARIOS_SQL = """
    SELECT usuario, unidade, COUNT(*) AS cnt
    FROM sei_processo.sei_atividades
    WHERE protocolo_formatado = ANY(%s)
      AND usuario IS NOT NULL
    GROUP BY usuario, unidade
    ORDER BY usuario, cnt DESC
"""

# ---------------------------------------------------------------------------
# Cypher - Schema
# ---------------------------------------------------------------------------
SETUP_CONSTRAINTS = [
    "CREATE CONSTRAINT processo_protocolo IF NOT EXISTS FOR (p:Processo) REQUIRE p.protocolo_formatado IS UNIQUE",
    "CREATE CONSTRAINT atividade_source_id IF NOT EXISTS FOR (a:Atividade) REQUIRE a.source_id IS UNIQUE",
    "CREATE CONSTRAINT unidade_sigla IF NOT EXISTS FOR (u:Unidade) REQUIRE u.sigla IS UNIQUE",
    "CREATE CONSTRAINT tipo_procedimento_nome IF NOT EXISTS FOR (tp:TipoProcedimento) REQUIRE tp.nome IS UNIQUE",
    "CREATE CONSTRAINT tipo_acao_chave IF NOT EXISTS FOR (ta:TipoAcao) REQUIRE ta.chave IS UNIQUE",
    "CREATE CONSTRAINT grupo_atividade_chave IF NOT EXISTS FOR (ga:GrupoAtividade) REQUIRE ga.chave IS UNIQUE",
    "CREATE CONSTRAINT orgao_sigla IF NOT EXISTS FOR (o:Orgao) REQUIRE o.sigla IS UNIQUE",
    "CREATE CONSTRAINT usuario_nome IF NOT EXISTS FOR (u:Usuario) REQUIRE u.nome IS UNIQUE",
    "CREATE CONSTRAINT documento_numero IF NOT EXISTS FOR (d:Documento) REQUIRE d.numero IS UNIQUE",
    "CREATE INDEX atividade_data IF NOT EXISTS FOR (a:Atividade) ON (a.data_hora)",
    "CREATE INDEX processo_data_criacao IF NOT EXISTS FOR (p:Processo) ON (p.data_criacao)",
    "CREATE INDEX atividade_seq IF NOT EXISTS FOR (a:Atividade) ON (a.seq)",
]

# ---------------------------------------------------------------------------
# Cypher - Phase A: Pre-create shared dimension nodes (single-threaded)
# ---------------------------------------------------------------------------
SEED_GRUPOS_CYPHER = """
UNWIND $groups AS g
MERGE (ga:GrupoAtividade {chave: g.chave})
SET ga.label = g.label, ga.horas = g.horas
"""

SEED_TIPOS_CYPHER = """
UNWIND $types AS t
MERGE (ta:TipoAcao {chave: t.chave})
WITH ta, t
MATCH (ga:GrupoAtividade {chave: t.grupo})
MERGE (ta)-[:PERTENCE_AO_GRUPO]->(ga)
"""

PRECREATE_ORGAOS_CYPHER = """
UNWIND $orgaos AS o
MERGE (:Orgao {sigla: o})
"""

PRECREATE_UNIDADES_CYPHER = """
UNWIND $units AS u
MERGE (:Unidade {sigla: u.sigla})
"""

LINK_UNIDADE_ORGAO_CYPHER = """
UNWIND $links AS l
MATCH (u:Unidade {sigla: l.unidade})
MATCH (o:Orgao {sigla: l.orgao})
MERGE (u)-[:PERTENCE_AO_ORGAO]->(o)
"""

LINK_SUBUNIDADE_CYPHER = """
UNWIND $links AS l
MATCH (child:Unidade {sigla: l.child})
MATCH (parent:Unidade {sigla: l.parent})
MERGE (child)-[:SUBUNIDADE_DE]->(parent)
"""

PRECREATE_TIPO_PROCEDIMENTOS_CYPHER = """
UNWIND $tipos AS t
MERGE (:TipoProcedimento {nome: t})
"""

PRECREATE_PROCESSOS_CYPHER = """
UNWIND $rows AS r
MERGE (p:Processo {protocolo_formatado: r.protocolo_formatado})
ON CREATE SET p.data_criacao = datetime(r.data_criacao)
WITH p, r
WHERE r.tipo_procedimento IS NOT NULL
MERGE (tp:TipoProcedimento {nome: r.tipo_procedimento})
MERGE (p)-[:TEM_TIPO]->(tp)
"""

PRECREATE_PROCESSO_UNIDADE_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (u:Unidade {sigla: r.unidade})
MERGE (p)-[:CRIADO_NA_UNIDADE]->(u)
"""

PRECREATE_PROCESSO_ORGAO_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (o:Orgao {sigla: r.orgao})
MERGE (p)-[:CRIADO_NO_ORGAO]->(o)
"""

PRECREATE_USUARIOS_CYPHER = """
UNWIND $users AS u
MERGE (usr:Usuario {nome: u.nome})
WITH usr, u
MATCH (o:Orgao {sigla: u.orgao})
MERGE (usr)-[:PERTENCE_AO_ORGAO]->(o)
"""

# ---------------------------------------------------------------------------
# Cypher - Phase B: Load atividades (parallel, no shared-node MERGE)
# ---------------------------------------------------------------------------
LOAD_ATIVIDADES_CYPHER = """
UNWIND $rows AS row

MATCH (proc:Processo {protocolo_formatado: row.protocolo_formatado})
MATCH (uni:Unidade {sigla: row.unidade})
MATCH (ta:TipoAcao {chave: row.tipo_acao})
MATCH (usr:Usuario {nome: row.usuario})

MERGE (atv:Atividade {source_id: row.source_id})
ON CREATE SET
    atv.data_hora = datetime(row.data_hora),
    atv.descricao = row.descricao,
    atv.tipo_acao = row.tipo_acao,
    atv.grupo = row.grupo,
    atv.ref_id = row.ref_id,
    atv.seq = row.seq

MERGE (atv)-[:DO_PROCESSO]->(proc)
MERGE (atv)-[:EXECUTADO_PELA_UNIDADE]->(uni)
MERGE (atv)-[:TIPO_ACAO]->(ta)
MERGE (atv)-[:EXECUTADO_PELO_USUARIO]->(usr)
"""

LOAD_TRAMITACAO_CYPHER = """
UNWIND $rows AS row
MATCH (atv:Atividade {source_id: row.source_id})
MATCH (src:Unidade {sigla: row.source_unidade})
MERGE (atv)-[:REMETIDO_PELA_UNIDADE]->(src)
"""

LOAD_DOCUMENTO_CYPHER = """
UNWIND $rows AS row
MATCH (atv:Atividade {source_id: row.source_id})
MERGE (doc:Documento {numero: row.numero})
ON CREATE SET doc.tipo = row.tipo,
              doc.serie_id = row.serie_id
MERGE (atv)-[:REFERENCIA_DOCUMENTO]->(doc)
"""

# ---------------------------------------------------------------------------
# Cypher - Phase D: Permanencia
# ---------------------------------------------------------------------------
LOAD_PERMANENCIA_UNIDADE_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (u:Unidade {sigla: r.unidade})
MERGE (p)-[rel:PASSOU_PELA_UNIDADE]->(u)
SET rel.duracao_total_horas = r.duracao_total_horas,
    rel.visitas = r.visitas,
    rel.primeira_entrada = datetime(r.primeira_entrada),
    rel.ultima_saida = datetime(r.ultima_saida)
"""

LOAD_PERMANENCIA_ORGAO_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (o:Orgao {sigla: r.orgao})
MERGE (p)-[rel:PASSOU_PELO_ORGAO]->(o)
SET rel.duracao_total_horas = r.duracao_total_horas,
    rel.visitas = r.visitas,
    rel.primeira_entrada = datetime(r.primeira_entrada),
    rel.ultima_saida = datetime(r.ultima_saida)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _neo4j_run_with_retry(driver, cypher, **kwargs):
    """Run a Cypher statement with exponential backoff + jitter on deadlock."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with driver.session() as session:
                session.run(cypher, **kwargs)
            return
        except TransientError:
            if attempt == MAX_RETRIES:
                raise
            wait = min(0.1 * (2 ** attempt) + random.uniform(0, 1.0), 10)
            log.debug("Deadlock (attempt %d/%d), retrying in %.1fs", attempt, MAX_RETRIES, wait)
            time.sleep(wait)


def _make_pg_conn():
    """Create a new PostgreSQL connection."""
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user=PG_USER,
        password=PG_PASSWORD, database=PG_DATABASE,
    )
    conn.autocommit = False
    return conn


def _parent_unidade(sigla: str) -> str | None:
    """Get parent unidade: 'SEAD-PI/GAB/NTGD' -> 'SEAD-PI/GAB'."""
    parts = sigla.split("/")
    if len(parts) <= 1:
        return None
    return "/".join(parts[:-1])


def _all_ancestor_unidades(sigla: str) -> list[str]:
    """Get all ancestors: 'A/B/C' -> ['A/B', 'A']."""
    ancestors = []
    current = sigla
    while True:
        parent = _parent_unidade(current)
        if parent is None:
            break
        ancestors.append(parent)
        current = parent
    return ancestors


def transform_row(row: dict, seq: int) -> dict:
    """Transform a PostgreSQL row into Cypher parameters."""
    descricao = row["descricao_replace"] or ""
    tipo_acao = classify_descricao(descricao)
    grupo = get_grupo(tipo_acao)
    is_creation = tipo_acao == "GERACAO-PROCEDIMENTO"
    source_unidade = extract_source_unidade(descricao) if tipo_acao == "PROCESSO-REMETIDO-UNIDADE" else None
    ref_id = extract_reference_id(descricao)
    doc_info = extract_document_info(descricao)

    data_hora_str = None
    if row["data_hora"]:
        dt = row["data_hora"]
        data_hora_str = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt)

    return {
        "source_id": row["id"],
        "protocolo_formatado": row["protocolo_formatado"],
        "data_hora": data_hora_str,
        "unidade": row["unidade"] or "DESCONHECIDA",
        "usuario": row["usuario"] or "DESCONHECIDO",
        "tipo_procedimento": row["tipo_procedimento"],
        "descricao": descricao,
        "tipo_acao": tipo_acao,
        "grupo": grupo,
        "is_creation": is_creation,
        "source_unidade": source_unidade,
        "ref_id": ref_id,
        "doc_info": doc_info,
        "seq": seq,
    }


# ---------------------------------------------------------------------------
# Phase A: Pre-create shared nodes (single-threaded, no deadlocks)
# ---------------------------------------------------------------------------
def precreate_shared_nodes(driver, processos, all_unidades: set[str],
                           user_orgao: dict[str, str]):
    """Create all dimension nodes before parallel atividade loading."""
    log.info("Phase A: Pre-creating shared dimension nodes...")

    # Grupos + TipoAcao
    grupos = get_all_grupo_records()
    tipos = get_all_tipo_acao_records()
    with driver.session() as session:
        session.run(SEED_GRUPOS_CYPHER, groups=grupos)
        session.run(SEED_TIPOS_CYPHER, types=tipos)
    log.info("  Seeded %d groups and %d action types", len(grupos), len(tipos))

    # Orgaos (extracted from unidade siglas)
    all_orgaos = list({extract_orgao(u) for u in all_unidades})
    with driver.session() as session:
        session.run(PRECREATE_ORGAOS_CYPHER, orgaos=all_orgaos)
    log.info("  Pre-created %d orgaos", len(all_orgaos))

    # Unidades (batched)
    unit_list = [{"sigla": u} for u in all_unidades]
    for i in range(0, len(unit_list), 1000):
        batch = unit_list[i:i + 1000]
        with driver.session() as session:
            session.run(PRECREATE_UNIDADES_CYPHER, units=batch)
    log.info("  Pre-created %d unidades", len(unit_list))

    # Link Unidade -[:PERTENCE_AO_ORGAO]-> Orgao
    unidade_orgao_links = [{"unidade": u, "orgao": extract_orgao(u)} for u in all_unidades]
    for i in range(0, len(unidade_orgao_links), 1000):
        batch = unidade_orgao_links[i:i + 1000]
        with driver.session() as session:
            session.run(LINK_UNIDADE_ORGAO_CYPHER, links=batch)
    log.info("  Linked unidades to orgaos")

    # Link Unidade -[:SUBUNIDADE_DE]-> Unidade (hierarchy)
    subunidade_links = []
    for u in all_unidades:
        parent = _parent_unidade(u)
        if parent and parent in all_unidades:
            subunidade_links.append({"child": u, "parent": parent})
    for i in range(0, len(subunidade_links), 1000):
        batch = subunidade_links[i:i + 1000]
        with driver.session() as session:
            session.run(LINK_SUBUNIDADE_CYPHER, links=batch)
    log.info("  Linked %d subunidade relationships", len(subunidade_links))

    # Processos + TipoProcedimento (batched)
    proc_rows = []
    for row in processos:
        data_criacao_str = None
        if row["data_hora"]:
            dt = row["data_hora"]
            data_criacao_str = dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt)
        proc_rows.append({
            "protocolo_formatado": row["protocolo_formatado"],
            "tipo_procedimento": row["tipo_procedimento"],
            "data_criacao": data_criacao_str,
        })
    for i in range(0, len(proc_rows), 1000):
        batch = proc_rows[i:i + 1000]
        with driver.session() as session:
            session.run(PRECREATE_PROCESSOS_CYPHER, rows=batch)
    log.info("  Pre-created %d processos (with data_criacao)", len(proc_rows))

    # Processo -[:CRIADO_NA_UNIDADE]-> Unidade
    creation_rows = [
        {
            "protocolo_formatado": row["protocolo_formatado"],
            "unidade": row["unidade"] or "DESCONHECIDA",
        }
        for row in processos
        if row["unidade"]
    ]
    for i in range(0, len(creation_rows), 1000):
        batch = creation_rows[i:i + 1000]
        with driver.session() as session:
            session.run(PRECREATE_PROCESSO_UNIDADE_CYPHER, rows=batch)
    log.info("  Linked processos to creation unidades")

    # Processo -[:CRIADO_NO_ORGAO]-> Orgao
    orgao_rows = [
        {
            "protocolo_formatado": row["protocolo_formatado"],
            "orgao": extract_orgao(row["unidade"]) if row["unidade"] else "DESCONHECIDO",
        }
        for row in processos
        if row["unidade"]
    ]
    for i in range(0, len(orgao_rows), 1000):
        batch = orgao_rows[i:i + 1000]
        with driver.session() as session:
            session.run(PRECREATE_PROCESSO_ORGAO_CYPHER, rows=batch)
    log.info("  Linked processos to creation orgaos")

    # Usuarios -[:PERTENCE_AO_ORGAO]-> Orgao
    user_rows = [{"nome": u, "orgao": o} for u, o in user_orgao.items()]
    for i in range(0, len(user_rows), 1000):
        batch = user_rows[i:i + 1000]
        with driver.session() as session:
            session.run(PRECREATE_USUARIOS_CYPHER, users=batch)
    log.info("  Pre-created %d usuarios (linked to orgaos)", len(user_rows))

    log.info("Phase A complete")


# ---------------------------------------------------------------------------
# Phase B: Load atividades (parallel, shared nodes already exist)
# ---------------------------------------------------------------------------
def load_atividades_batch(driver, transformed: list[dict]):
    """Load a batch of atividades using MATCH for shared nodes."""
    _neo4j_run_with_retry(driver, LOAD_ATIVIDADES_CYPHER, rows=transformed)

    tramitacao_rows = [
        {"source_id": t["source_id"], "source_unidade": t["source_unidade"]}
        for t in transformed
        if t["source_unidade"]
    ]
    if tramitacao_rows:
        _neo4j_run_with_retry(driver, LOAD_TRAMITACAO_CYPHER, rows=tramitacao_rows)

    documento_rows = [
        {
            "source_id": t["source_id"],
            "numero": t["doc_info"]["numero"],
            "tipo": t["doc_info"]["tipo"],
            "serie_id": t["doc_info"]["serie_id"],
        }
        for t in transformed
        if t["doc_info"]
    ]
    if documento_rows:
        _neo4j_run_with_retry(driver, LOAD_DOCUMENTO_CYPHER, rows=documento_rows)


def process_chunk(protocolo_ids: list[str], neo4j_driver, dry_run: bool, batch_size: int) -> tuple[int, Counter, list[str], set[str]]:
    """Worker: fetch andamentos for a chunk, classify, load atividades."""
    pg_conn = _make_pg_conn()
    total_rows = 0
    stats: Counter = Counter()
    unclassified: list[str] = []
    unidades: set[str] = set()

    seq_counters: dict[str, int] = {}

    try:
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        # Debug: verify query returns data
        cursor.execute("SELECT COUNT(*) FROM sei_processo.sei_atividades WHERE protocolo_formatado = ANY(%s)", (protocolo_ids,))
        debug_count = cursor.fetchone()[0]
        log.info("  Chunk debug: %d IDs, %d matching rows, first IDs: %s, types: %s",
                  len(protocolo_ids), debug_count, protocolo_ids[:3],
                  [type(x).__name__ for x in protocolo_ids[:3]])
        if debug_count == 0:
            log.warning("  Chunk has 0 matching rows for %d IDs! Sample IDs: %s", len(protocolo_ids), protocolo_ids[:5])

        cursor.execute(FETCH_ANDAMENTOS_SQL, (protocolo_ids,))

        batch = []
        for row in cursor:
            d = dict(row)
            pf = d["protocolo_formatado"]
            seq_counters[pf] = seq_counters.get(pf, 0) + 1
            t = transform_row(d, seq_counters[pf])
            batch.append(t)
            stats[t["tipo_acao"]] += 1
            unidades.add(t["unidade"])
            if t["source_unidade"]:
                unidades.add(t["source_unidade"])
            if t["tipo_acao"] == "OUTROS" and len(unclassified) < 20:
                desc = t["descricao"][:100]
                if desc not in unclassified:
                    unclassified.append(desc)

            if len(batch) >= batch_size:
                total_rows += len(batch)
                if not dry_run:
                    load_atividades_batch(neo4j_driver, batch)
                batch = []

        if batch:
            total_rows += len(batch)
            if not dry_run:
                load_atividades_batch(neo4j_driver, batch)

        cursor.close()
    finally:
        pg_conn.close()

    return total_rows, stats, unclassified, unidades



# ---------------------------------------------------------------------------
# Phase C: Build timeline DAG
# ---------------------------------------------------------------------------
LOAD_TIMELINE_CYPHER = """
UNWIND $edges AS e
MATCH (a1:Atividade {source_id: e.from_id})
MATCH (a2:Atividade {source_id: e.to_id})
MERGE (a1)-[r:SEGUIDA_POR]->(a2)
SET r.mesma_unidade = e.mesma_unidade
MERGE (a2)-[r2:PRECEDIDA_POR]->(a1)
SET r2.mesma_unidade = e.mesma_unidade
"""

LOAD_INICIO_CYPHER = """
UNWIND $rows AS r
MATCH (p:Processo {protocolo_formatado: r.protocolo_formatado})
MATCH (a:Atividade {source_id: r.first_id})
MERGE (p)-[:INICIOU_PROCESSO]->(a)
"""

LOAD_INDEPENDENT_TIMELINE_CYPHER = """
UNWIND $edges AS e
MATCH (a1:Atividade {source_id: e.from_id})
MATCH (a2:Atividade {source_id: e.to_id})
MERGE (a1)-[r:SEGUIDO_INDEPENDENTEMENTE_POR]->(a2)
SET r.ref_id = e.ref_id
"""


CONCLUSION_TYPES = {
    "CONCLUSAO-AUTOMATICA-UNIDADE",
    "CONCLUSAO-PROCESSO-UNIDADE",
}

TRANSFER_TYPES = {
    "CONCLUSAO-AUTOMATICA-UNIDADE",
    "CONCLUSAO-PROCESSO-UNIDADE",
    "PROCESSO-REMETIDO-UNIDADE",
    "PROCESSO-RECEBIDO-UNIDADE",
}


def _transfer_priority(tipo_acao: str) -> int:
    """Logical priority for transfer events at the same timestamp.
    conclusão(0) → remetido(1) → recebido(2) → others(1)
    """
    if tipo_acao in CONCLUSION_TYPES:
        return 0
    if tipo_acao == "PROCESSO-REMETIDO-UNIDADE":
        return 1
    if tipo_acao == "PROCESSO-RECEBIDO-UNIDADE":
        return 2
    return 1


def _sort_and_fix_activities(activities: list[dict]) -> list[dict]:
    """Sort activities with same-timestamp priority tiebreaker and
    post-sort fixup for misrecorded recebido/remetido pairs within 60s.

    Mirrors the frontend logic in process-flow-utils.ts.
    """
    # Activities arrive pre-sorted by (data_hora, source_id) from Cypher.
    # Re-sort with priority tiebreaker for same-timestamp events.
    activities.sort(key=lambda a: (
        a.get("data_hora", ""),
        _transfer_priority(a["tipo_acao"]),
        a["source_id"],
    ))

    # Post-sort fixup: when a RECEBIDO appears before its matching
    # REMETIDO/CONCLUSÃO within 60s, move them before the RECEBIDO.
    i = 0
    while i < len(activities):
        if activities[i]["tipo_acao"] != "PROCESSO-RECEBIDO-UNIDADE":
            i += 1
            continue
        recebido_dt = activities[i].get("data_hora", "")
        j = i + 1
        while j < len(activities):
            candidate = activities[j]
            cand_dt = candidate.get("data_hora", "")
            # ISO strings are comparable; check ~60s window
            if cand_dt and recebido_dt and cand_dt > recebido_dt[:17]:
                # Past the same minute+1 — stop looking
                break
            if candidate["tipo_acao"] in (
                "PROCESSO-REMETIDO-UNIDADE",
                "CONCLUSAO-AUTOMATICA-UNIDADE",
                "CONCLUSAO-PROCESSO-UNIDADE",
            ):
                # Move before the recebido
                activities.insert(i, activities.pop(j))
                # Don't advance i — re-check from same position
            else:
                j += 1
        i += 1

    return activities


def _build_edges_for_processo(activities: list[dict]) -> tuple[list[dict], list[dict]]:
    """Build SEGUIDA_POR edges using unidade-context tracking.

    Returns (flow_edges, independent_edges):
      - flow_edges: SEGUIDA_POR edges between activities in the formal flow
      - independent_edges: SEGUIDO_INDEPENDENTEMENTE_POR edges linking
        activities via shared bloco/document reference across units

    Rules:
      1. Same unidade as previous: chain to last activity there
      2. REMETIDO: logged at destination unidade, connect from last
         activity at the SOURCE unidade (extracted from description)
      3. RECEBIDO: connect from pending REMETIDO(s) that targeted
         this unidade
      4. Activities from units that never had GERACAO/RECEBIDO/REABERTURA
         are linked independently via shared bloco/document reference
      5. Conclusão nodes do NOT produce outgoing SEGUIDA_POR edges
      6. Cross-unit document references create independent edges for
         all units (activated or not)

    This correctly handles parallel branches: activities at GAMIL
    only chain to other activities at GAMIL, never cross to SESAPI.
    """
    if len(activities) < 2:
        return [], []

    # Fix 1: Sort with priority tiebreaker + 60s fixup
    activities = _sort_and_fix_activities(activities)

    # Determine activated units (formally part of the flow)
    FLOW_ACTIVATION_TYPES = {
        "GERACAO-PROCEDIMENTO",
        "PROCESSO-RECEBIDO-UNIDADE",
        "REABERTURA-PROCESSO-UNIDADE",
    }
    activated_units: set[str] = set()
    for atv in activities:
        if atv["tipo_acao"] in FLOW_ACTIVATION_TYPES:
            activated_units.add(atv["unidade"])

    # Index: first activity per ref_id (across all units) for independent linking
    first_by_ref: dict[str, dict] = {}
    for atv in activities:
        rid = atv.get("ref_id")
        if rid and rid not in first_by_ref:
            first_by_ref[rid] = atv

    flow_edges = []
    independent_edges = []
    # Track the last activity at each unidade
    last_at: dict[str, dict] = {}
    # Track pending remetidos by destination unidade
    pending_remetidos: dict[str, list[dict]] = {}

    for atv in activities:
        u = atv["unidade"]
        tipo = atv["tipo_acao"]
        sid = atv["source_id"]

        # Fix 3: Cross-unit document/bloco reference (all units, not just non-activated)
        rid = atv.get("ref_id")
        if rid and rid in first_by_ref:
            origin = first_by_ref[rid]
            if origin["source_id"] != sid and origin["unidade"] != u:
                independent_edges.append({
                    "from_id": origin["source_id"],
                    "to_id": sid,
                    "ref_id": rid,
                })

        # Non-activated unit: skip normal flow connections
        if u not in activated_units:
            continue

        if tipo == "PROCESSO-REMETIDO-UNIDADE":
            # REMETIDO is logged at the destination unidade.
            # Connect from last activity at the SOURCE unidade.
            src = atv.get("source_unidade")
            if src and src in last_at:
                # Fix 2: skip if last at source was a conclusão
                if last_at[src]["tipo_acao"] not in CONCLUSION_TYPES:
                    flow_edges.append({
                        "from_id": last_at[src]["source_id"],
                        "to_id": sid,
                        "mesma_unidade": False,
                    })
            elif u in last_at:
                # Fallback: connect from last at same unidade
                if last_at[u]["tipo_acao"] not in CONCLUSION_TYPES:
                    flow_edges.append({
                        "from_id": last_at[u]["source_id"],
                        "to_id": sid,
                        "mesma_unidade": True,
                    })
            # Track for RECEBIDO matching
            if u not in pending_remetidos:
                pending_remetidos[u] = []
            pending_remetidos[u].append(atv)

        elif tipo == "PROCESSO-RECEBIDO-UNIDADE":
            # RECEBIDO at unidade u. Connect from pending remetido(s).
            if u in pending_remetidos and pending_remetidos[u]:
                for rem in pending_remetidos[u]:
                    flow_edges.append({
                        "from_id": rem["source_id"],
                        "to_id": sid,
                        "mesma_unidade": True,
                    })
                pending_remetidos[u] = []
            elif u in last_at:
                if last_at[u]["tipo_acao"] not in CONCLUSION_TYPES:
                    flow_edges.append({
                        "from_id": last_at[u]["source_id"],
                        "to_id": sid,
                        "mesma_unidade": True,
                    })

        else:
            # Regular activity: chain to last at same unidade
            # Fix 2: skip if last was a conclusão
            if u in last_at and last_at[u]["tipo_acao"] not in CONCLUSION_TYPES:
                flow_edges.append({
                    "from_id": last_at[u]["source_id"],
                    "to_id": sid,
                    "mesma_unidade": True,
                })

        last_at[u] = atv

    return flow_edges, independent_edges


def build_timeline(driver):
    """Build SEGUIDA_POR/PRECEDIDA_POR DAG + INICIOU_PROCESSO.

    Uses unidade-context tracking instead of naive timestamp grouping.
    Activities chain within the same unidade; transitions happen via
    REMETIDO->RECEBIDO pairs. Parallel branches stay independent.
    """
    log.info("Phase C: Building timeline DAG (unidade-context)...")

    with driver.session() as session:
        result = session.run("MATCH (p:Processo) RETURN count(p) AS cnt")
        total = result.single()["cnt"]

    log.info("  Processing %d processos...", total)

    skip = 0
    batch_size = 500
    total_edges = 0
    total_inicio = 0

    while skip < total:
        with driver.session() as session:
            result = session.run("""
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
            """, skip=skip, limit=batch_size)

            all_flow_edges = []
            all_independent_edges = []
            inicio_rows = []
            for record in result:
                pf = record["pf"]
                activities = record["activities"]
                if not activities:
                    continue

                flow_edges, independent_edges = _build_edges_for_processo(activities)
                all_flow_edges.extend(flow_edges)
                all_independent_edges.extend(independent_edges)

                inicio_rows.append({
                    "protocolo_formatado": pf,
                    "first_id": activities[0]["source_id"],
                })

        if all_flow_edges:
            for i in range(0, len(all_flow_edges), 500):
                sub = all_flow_edges[i:i + 500]
                _neo4j_run_with_retry(driver, LOAD_TIMELINE_CYPHER, edges=sub)
            total_edges += len(all_flow_edges)

        if all_independent_edges:
            for i in range(0, len(all_independent_edges), 500):
                sub = all_independent_edges[i:i + 500]
                _neo4j_run_with_retry(driver, LOAD_INDEPENDENT_TIMELINE_CYPHER, edges=sub)
            total_edges += len(all_independent_edges)

        if inicio_rows:
            for i in range(0, len(inicio_rows), 500):
                sub = inicio_rows[i:i + 500]
                _neo4j_run_with_retry(driver, LOAD_INICIO_CYPHER, rows=sub)
            total_inicio += len(inicio_rows)

        skip += batch_size
        if skip % 5000 == 0 or skip >= total:
            log.info("  Progress: %d/%d processos, %d edges, %d inicio",
                     min(skip, total), total, total_edges, total_inicio)

    log.info("Phase C complete: %d edges (SEGUIDA_POR + SEGUIDO_INDEPENDENTEMENTE_POR) + %d INICIOU_PROCESSO",
             total_edges, total_inicio)

# ---------------------------------------------------------------------------
# Phase D: Compute permanencia (PASSOU_PELA_UNIDADE + PASSOU_PELO_ORGAO)
# ---------------------------------------------------------------------------
def compute_permanencia(driver):
    """Compute time each processo spent per unidade and per orgao.

    Uses stint-based grouping: consecutive activities at the same unidade
    form a stint. If a processo visits A->B->A, unidade A gets two stints summed.
    Orgao permanencia is aggregated from unidade stints.
    """
    log.info("Phase D: Computing permanencia (PASSOU_PELA_UNIDADE + PASSOU_PELO_ORGAO)...")

    with driver.session() as session:
        result = session.run("MATCH (p:Processo) RETURN count(p) AS cnt")
        total = result.single()["cnt"]

    log.info("  Computing for %d processos...", total)

    skip = 0
    batch_size = 1000
    total_unidade_links = 0
    total_orgao_links = 0

    while skip < total:
        with driver.session() as session:
            result = session.run("""
                MATCH (p:Processo)
                WITH p ORDER BY p.protocolo_formatado SKIP $skip LIMIT $limit
                MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
                MATCH (a)-[:EXECUTADO_PELA_UNIDADE]->(u:Unidade)
                WITH p.protocolo_formatado AS pf, a.data_hora AS dt, u.sigla AS unidade, a.source_id AS sid
                ORDER BY pf, dt, sid
                RETURN pf, collect({data_hora: toString(dt), unidade: unidade}) AS timeline
            """, skip=skip, limit=batch_size)

            unidade_rows = []
            orgao_rows = []
            for record in result:
                pf = record["pf"]
                timeline = record["timeline"]

                if not timeline:
                    continue

                # Compute stints: group consecutive same-unidade entries
                stints = []
                cur_u = timeline[0]["unidade"]
                cur_start = timeline[0]["data_hora"]
                cur_end = cur_start

                for entry in timeline[1:]:
                    if entry["unidade"] == cur_u:
                        cur_end = entry["data_hora"]
                    else:
                        stints.append({"unidade": cur_u, "entrada": cur_start, "saida": cur_end})
                        cur_u = entry["unidade"]
                        cur_start = entry["data_hora"]
                        cur_end = cur_start
                stints.append({"unidade": cur_u, "entrada": cur_start, "saida": cur_end})

                # Aggregate stints by unidade
                agg_unidade: dict[str, dict] = {}
                for stint in stints:
                    u = stint["unidade"]
                    start_dt = datetime.fromisoformat(stint["entrada"])
                    end_dt = datetime.fromisoformat(stint["saida"])
                    dur_h = (end_dt - start_dt).total_seconds() / 3600

                    if u not in agg_unidade:
                        agg_unidade[u] = {
                            "duracao_total_horas": 0.0,
                            "visitas": 0,
                            "primeira_entrada": stint["entrada"],
                            "ultima_saida": stint["saida"],
                        }
                    agg_unidade[u]["duracao_total_horas"] += dur_h
                    agg_unidade[u]["visitas"] += 1
                    agg_unidade[u]["ultima_saida"] = stint["saida"]

                for u, stats in agg_unidade.items():
                    unidade_rows.append({
                        "protocolo_formatado": pf,
                        "unidade": u,
                        "duracao_total_horas": round(stats["duracao_total_horas"], 2),
                        "visitas": stats["visitas"],
                        "primeira_entrada": stats["primeira_entrada"],
                        "ultima_saida": stats["ultima_saida"],
                    })

                # Aggregate stints by orgao
                agg_orgao: dict[str, dict] = {}
                for stint in stints:
                    orgao = extract_orgao(stint["unidade"])
                    start_dt = datetime.fromisoformat(stint["entrada"])
                    end_dt = datetime.fromisoformat(stint["saida"])
                    dur_h = (end_dt - start_dt).total_seconds() / 3600

                    if orgao not in agg_orgao:
                        agg_orgao[orgao] = {
                            "duracao_total_horas": 0.0,
                            "visitas": 0,
                            "primeira_entrada": stint["entrada"],
                            "ultima_saida": stint["saida"],
                        }
                    agg_orgao[orgao]["duracao_total_horas"] += dur_h
                    agg_orgao[orgao]["visitas"] += 1
                    agg_orgao[orgao]["ultima_saida"] = stint["saida"]

                for o, stats in agg_orgao.items():
                    orgao_rows.append({
                        "protocolo_formatado": pf,
                        "orgao": o,
                        "duracao_total_horas": round(stats["duracao_total_horas"], 2),
                        "visitas": stats["visitas"],
                        "primeira_entrada": stats["primeira_entrada"],
                        "ultima_saida": stats["ultima_saida"],
                    })

        # Write PASSOU_PELA_UNIDADE
        if unidade_rows:
            for i in range(0, len(unidade_rows), 500):
                sub = unidade_rows[i:i + 500]
                _neo4j_run_with_retry(driver, LOAD_PERMANENCIA_UNIDADE_CYPHER, rows=sub)
            total_unidade_links += len(unidade_rows)

        # Write PASSOU_PELO_ORGAO
        if orgao_rows:
            for i in range(0, len(orgao_rows), 500):
                sub = orgao_rows[i:i + 500]
                _neo4j_run_with_retry(driver, LOAD_PERMANENCIA_ORGAO_CYPHER, rows=sub)
            total_orgao_links += len(orgao_rows)

        skip += batch_size
        if skip % 5000 == 0 or skip >= total:
            log.info("  Progress: %d/%d processos, %d PASSOU_PELA_UNIDADE, %d PASSOU_PELO_ORGAO",
                     min(skip, total), total, total_unidade_links, total_orgao_links)

    log.info("Phase D complete: %d PASSOU_PELA_UNIDADE + %d PASSOU_PELO_ORGAO",
             total_unidade_links, total_orgao_links)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="ETL: sei_atividades -> Neo4j")
    parser.add_argument("--from", dest="from_date", type=str, help="Filter from date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=str, help="Filter to date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Classify only, no Neo4j writes")
    parser.add_argument("--batch-size", type=int, default=500, help="Neo4j batch size (default: 500)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (default: 8)")
    parser.add_argument("--chunk-size", type=int, default=200, help="Processos per worker chunk (default: 200)")
    parser.add_argument("--skip-timeline", action="store_true", help="Skip building SEGUIDA_POR DAG")
    parser.add_argument("--skip-permanencia", action="store_true", help="Skip computing permanencia")
    args = parser.parse_args()

    # -- Phase 1: Find processos --
    log.info("Phase 1: Finding processos (creation events)...")

    date_filter = ""
    params: list = []
    if args.from_date and args.to_date:
        date_filter = "AND data_hora >= %s AND data_hora < %s"
        params = [args.from_date, args.to_date]
    elif args.from_date:
        date_filter = "AND data_hora >= %s"
        params = [args.from_date]
    elif args.to_date:
        date_filter = "AND data_hora < %s"
        params = [args.to_date]

    pg_conn = _make_pg_conn()
    cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Debug: check table and data availability
    final_sql = FIND_PROCESSOS_SQL.format(date_filter=date_filter)
    log.info("SQL: %s", final_sql.strip()[:200])
    log.info("Params: %s", params)

    cursor.execute(final_sql, params)
    processos = [dict(row) for row in cursor.fetchall()]
    cursor.close()
    pg_conn.close()

    protocolo_ids = [row["protocolo_formatado"] for row in processos]
    log.info("Found %d processos", len(protocolo_ids))

    if not protocolo_ids:
        log.info("No processos found. Exiting.")
        return

    # -- Scan pass: collect all unidades from andamentos --
    log.info("Scanning andamentos for unidades and usuarios...")
    all_unidades: set[str] = {"DESCONHECIDA"}
    for row in processos:
        if row["unidade"]:
            all_unidades.add(row["unidade"])

    pg_conn = _make_pg_conn()
    cursor = pg_conn.cursor()

    cursor.execute("""
        SELECT DISTINCT unidade
        FROM sei_processo.sei_atividades
        WHERE protocolo_formatado = ANY(%s) AND unidade IS NOT NULL
    """, (protocolo_ids,))
    for (u,) in cursor:
        all_unidades.add(u)

    cursor.execute("""
        SELECT DISTINCT descricao_replace
        FROM sei_processo.sei_atividades
        WHERE protocolo_formatado = ANY(%s)
          AND descricao_replace LIKE 'Processo remetido%%'
    """, (protocolo_ids,))
    for (desc,) in cursor:
        src = extract_source_unidade(desc)
        if src:
            all_unidades.add(src)

    log.info("Found %d unique unidades", len(all_unidades))

    # Add ancestor unidades for SUBUNIDADE_DE hierarchy
    ancestors: set[str] = set()
    for u in list(all_unidades):
        for anc in _all_ancestor_unidades(u):
            ancestors.add(anc)
    all_unidades |= ancestors
    if ancestors:
        log.info("  Added %d ancestor unidades for hierarchy", len(ancestors - all_unidades | ancestors))

    # Scan for users and determine each user's primary orgao
    cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(FIND_USUARIOS_SQL, (protocolo_ids,))
    user_orgao: dict[str, str] = {"DESCONHECIDO": "DESCONHECIDO"}
    for row in cursor:
        usuario = row["usuario"]
        unidade = row["unidade"]
        if usuario not in user_orgao:
            user_orgao[usuario] = extract_orgao(unidade)
    cursor.close()
    pg_conn.close()
    log.info("Found %d unique usuarios", len(user_orgao))

    # -- Neo4j setup --
    neo4j_driver = None
    if not args.dry_run:
        log.info("Connecting to Neo4j: %s", NEO4J_URI)
        neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        neo4j_driver.verify_connectivity()
        log.info("Neo4j connected")

        with neo4j_driver.session() as session:
            for cypher in SETUP_CONSTRAINTS:
                session.run(cypher)
        log.info("Schema: %d constraints/indexes", len(SETUP_CONSTRAINTS))

        precreate_shared_nodes(neo4j_driver, processos, all_unidades, user_orgao)

    # -- Phase B: Load atividades in parallel --
    chunk_size = args.chunk_size
    chunks = [protocolo_ids[i:i + chunk_size] for i in range(0, len(protocolo_ids), chunk_size)]
    log.info("Phase B: Loading atividades for %d processos in %d chunks (%d workers)",
             len(protocolo_ids), len(chunks), args.workers)

    grand_total = 0
    grand_stats: Counter = Counter()
    all_unclassified: list[str] = []
    completed_chunks = 0
    failed_chunks = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(process_chunk, chunk, neo4j_driver, args.dry_run, args.batch_size): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            chunk_idx = futures[future]
            try:
                count, stats, unclassified, _units = future.result()
                grand_total += count
                grand_stats += stats
                for desc in unclassified:
                    if desc not in all_unclassified and len(all_unclassified) < 50:
                        all_unclassified.append(desc)
                completed_chunks += 1
                if completed_chunks % 10 == 0 or completed_chunks == len(chunks):
                    log.info("  Progress: %d/%d chunks (%d rows)", completed_chunks, len(chunks), grand_total)
            except Exception:
                log.exception("Failed chunk %d", chunk_idx)
                failed_chunks += 1

    log.info("Phase B complete: %d atividades loaded", grand_total)

    # -- Phase C: Build timeline --
    if neo4j_driver and not args.dry_run and not args.skip_timeline:
        build_timeline(neo4j_driver)

    # -- Phase D: Compute permanencia --
    if neo4j_driver and not args.dry_run and not args.skip_permanencia:
        compute_permanencia(neo4j_driver)

    # -- Stats --
    log.info("ETL complete. Total: %d atividades across %d processos", grand_total, len(protocolo_ids))
    if grand_stats:
        log.info("Classification distribution:")
        for tipo, count in grand_stats.most_common():
            log.info("  %-45s %6d", tipo, count)
    if all_unclassified:
        log.warning("Unclassified (%d unique):", len(all_unclassified))
        for desc in all_unclassified:
            log.warning("  %s", desc)
    if failed_chunks:
        log.error("Failed chunks: %d / %d", failed_chunks, len(chunks))

    if neo4j_driver:
        neo4j_driver.close()


if __name__ == "__main__":
    main()
