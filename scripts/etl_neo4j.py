"""
ETL script: PostgreSQL sei_atividades → Neo4j graph.

Graph model (enhanced for process flow clustering, shortest path & permanência):

  Nodes:
    - Processo         {protocolo_formatado*, data_criacao}
    - Atividade        {source_id*, data_hora, descricao, tipo_acao, grupo, ref_id, seq}
    - TipoProcedimento {nome*}
    - Unidade          {sigla*, id_unidade, descricao}
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
    - (Processo)-[:CONTEM_DOCUMENTO]->(Documento)            # processo contains this document
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
    - (Atividade)-[:SEGUIDA_POR {mesma_unidade, intervalo_horas, intervalo_dias}]->(Atividade)  # DAG timeline forward
    - (Atividade)-[:PRECEDIDA_POR {mesma_unidade, intervalo_horas, intervalo_dias}]->(Atividade) # DAG timeline backward
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
import csv
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

from etl_neo4j_classifier import (
    classify_descricao,
    extract_document_info,
    extract_reference_id,
    extract_source_unidade,
    get_all_grupo_records,
    get_all_tipo_acao_records,
    get_grupo,
)
from pipeline.classification import extract_orgao
from pipeline.cli import add_standard_args, resolve_settings
from pipeline.config import ConfigError, Settings
from pipeline.hierarchy import all_ancestor_unidades, parent_unidade
from pipeline.logging_setup import configure_logging
from pipeline.neo4j_driver import build_driver
from pipeline.postgres import make_pg_conn
from pipeline.readers import GraphReader, JsonFileReader, Neo4jReader
from pipeline.writers import DirectNeo4jWriter, GraphWriter, JsonFileWriter

log = configure_logging(__name__)

# Module-level settings handle, populated by main() so worker-scoped helpers
# (process_chunk → _make_pg_conn) can reach Postgres credentials without
# threading them through every function signature.
_SETTINGS: Settings | None = None

TZ = ZoneInfo("America/Fortaleza")

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
# Cypher - Phase A: composite templates (MERGE + chained MATCH/MERGE).
#
# Pure node and pure edge writes in Phase A go through writer.write_nodes /
# writer.write_edges, which generate their own MERGE Cypher from structured
# row dicts. Only the composite templates (MERGE + downstream MATCH + MERGE
# in a single statement) are kept as explicit constants here.
# ---------------------------------------------------------------------------
SEED_TIPOS_CYPHER = """
UNWIND $types AS t
MERGE (ta:TipoAcao {chave: t.chave})
WITH ta, t
MATCH (ga:GrupoAtividade {chave: t.grupo})
MERGE (ta)-[:PERTENCE_AO_GRUPO]->(ga)
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

PRECREATE_USUARIOS_CYPHER = """
UNWIND $users AS u
MERGE (usr:Usuario {nome: u.nome})
WITH usr, u
MATCH (o:Orgao {sigla: u.orgao})
MERGE (usr)-[:PERTENCE_AO_ORGAO]->(o)
"""

# ---------------------------------------------------------------------------
# Cypher - Phase B: composite load statements
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

# REMETIDO_PELA_UNIDADE is a clean MATCH+MATCH+MERGE edge — handled by
# writer.write_edges in load_atividades_batch, no constant needed here.

LOAD_DOCUMENTO_CYPHER = """
UNWIND $rows AS row
MATCH (atv:Atividade {source_id: row.source_id})
MERGE (doc:Documento {numero: row.numero})
ON CREATE SET doc.tipo = row.tipo,
              doc.serie_id = row.serie_id
MERGE (atv)-[:REFERENCIA_DOCUMENTO]->(doc)
WITH atv, doc
MATCH (atv)-[:DO_PROCESSO]->(p:Processo)
MERGE (p)-[:CONTEM_DOCUMENTO]->(doc)
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
def _make_pg_conn():
    """Open a Postgres connection using the module-level pipeline settings."""
    if _SETTINGS is None:
        raise RuntimeError("pipeline settings not initialized; main() must run first")
    return make_pg_conn(_SETTINGS)


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
def precreate_shared_nodes(writer: GraphWriter, processos, all_unidades: set[str],
                           user_orgao: dict[str, str]):
    """Create all dimension nodes before parallel atividade loading."""
    log.info("Phase A: Pre-creating shared dimension nodes...")
    writer.open_phase("A")

    # Grupos (clean MERGE) + TipoAcao (composite: MERGE + MATCH GrupoAtividade + MERGE edge)
    grupos = get_all_grupo_records()
    tipos = get_all_tipo_acao_records()
    writer.write_nodes("GrupoAtividade", ["chave"], grupos, phase="A", props=["label", "horas"])
    writer.execute_template("seed_tipos", SEED_TIPOS_CYPHER, {"types": tipos}, phase="A")
    log.info("  Seeded %d groups and %d action types", len(grupos), len(tipos))

    # Orgaos (extracted from unidade siglas)
    all_orgaos = sorted({extract_orgao(u) for u in all_unidades})
    writer.write_nodes(
        "Orgao", ["sigla"],
        [{"sigla": o} for o in all_orgaos],
        phase="A",
    )
    log.info("  Pre-created %d orgaos", len(all_orgaos))

    # Load CSV lookup for unidade enrichment
    _csv_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "notebooks", "cost", "unidades_sei.csv"
    )
    unidade_csv_lookup: dict[str, dict] = {}
    try:
        with open(_csv_path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                unidade_csv_lookup[row["Sigla"]] = {
                    "id_unidade": row["IdUnidade"],
                    "descricao": row["Descricao"],
                }
        log.info("  Loaded %d unidades from CSV lookup", len(unidade_csv_lookup))
    except FileNotFoundError:
        log.warning("  CSV file not found at %s – id_unidade/descricao will be None", _csv_path)

    # Unidades — writer handles UNWIND batching internally
    unit_list = [
        {
            "sigla": u,
            "id_unidade": unidade_csv_lookup.get(u, {}).get("id_unidade"),
            "descricao": unidade_csv_lookup.get(u, {}).get("descricao"),
        }
        for u in all_unidades
    ]
    writer.write_nodes(
        "Unidade", ["sigla"], unit_list,
        phase="A", props=["id_unidade", "descricao"],
    )
    log.info("  Pre-created %d unidades", len(unit_list))

    # Unidade -[:PERTENCE_AO_ORGAO]-> Orgao
    unidade_orgao_links = [{"unidade": u, "orgao": extract_orgao(u)} for u in all_unidades]
    writer.write_edges(
        "PERTENCE_AO_ORGAO",
        "Unidade", {"unidade": "sigla"},
        "Orgao", {"orgao": "sigla"},
        unidade_orgao_links,
        phase="A",
    )
    log.info("  Linked unidades to orgaos")

    # Unidade -[:SUBUNIDADE_DE]-> Unidade (hierarchy; same label on both ends)
    subunidade_links = []
    for u in all_unidades:
        parent = parent_unidade(u)
        if parent and parent in all_unidades:
            subunidade_links.append({"child": u, "parent": parent})
    writer.write_edges(
        "SUBUNIDADE_DE",
        "Unidade", {"child": "sigla"},
        "Unidade", {"parent": "sigla"},
        subunidade_links,
        phase="A",
    )
    log.info("  Linked %d subunidade relationships", len(subunidade_links))

    # Processos + TipoProcedimento (composite: MERGE Processo + conditional MERGE TipoProcedimento + MERGE edge)
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
        writer.execute_template("precreate_processos", PRECREATE_PROCESSOS_CYPHER, {"rows": batch}, phase="A")
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
    writer.write_edges(
        "CRIADO_NA_UNIDADE",
        "Processo", {"protocolo_formatado": "protocolo_formatado"},
        "Unidade", {"unidade": "sigla"},
        creation_rows,
        phase="A",
    )
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
    writer.write_edges(
        "CRIADO_NO_ORGAO",
        "Processo", {"protocolo_formatado": "protocolo_formatado"},
        "Orgao", {"orgao": "sigla"},
        orgao_rows,
        phase="A",
    )
    log.info("  Linked processos to creation orgaos")

    # Usuarios (composite: MERGE Usuario + MATCH Orgao + MERGE edge)
    user_rows = [{"nome": u, "orgao": o} for u, o in user_orgao.items()]
    for i in range(0, len(user_rows), 1000):
        batch = user_rows[i:i + 1000]
        writer.execute_template("precreate_usuarios", PRECREATE_USUARIOS_CYPHER, {"users": batch}, phase="A")
    log.info("  Pre-created %d usuarios (linked to orgaos)", len(user_rows))

    writer.close_phase("A")
    log.info("Phase A complete")


# ---------------------------------------------------------------------------
# Phase B: Load atividades (parallel, shared nodes already exist)
# ---------------------------------------------------------------------------
def load_atividades_batch(writer: GraphWriter, transformed: list[dict]):
    """Load a batch of atividades using MATCH for shared nodes."""
    # Composite: 4× MATCH + MERGE Atividade + 4× MERGE edge in one statement.
    writer.execute_template("load_atividades", LOAD_ATIVIDADES_CYPHER,
                             {"rows": transformed}, phase="B")

    tramitacao_rows = [
        {"source_id": t["source_id"], "source_unidade": t["source_unidade"]}
        for t in transformed
        if t["source_unidade"]
    ]
    if tramitacao_rows:
        writer.write_edges(
            "REMETIDO_PELA_UNIDADE",
            "Atividade", {"source_id": "source_id"},
            "Unidade", {"source_unidade": "sigla"},
            tramitacao_rows,
            phase="B",
        )

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
        # Composite: MATCH Atividade + MERGE Documento + MERGE edge + MATCH
        # Processo via edge + MERGE edge. Stays as a template.
        writer.execute_template("load_documento", LOAD_DOCUMENTO_CYPHER,
                                 {"rows": documento_rows}, phase="B")


def process_chunk(protocolo_ids: list[str], writer: GraphWriter | None, dry_run: bool, batch_size: int) -> tuple[int, Counter, list[str], set[str]]:
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
                    load_atividades_batch(writer, batch)
                batch = []

        if batch:
            total_rows += len(batch)
            if not dry_run:
                load_atividades_batch(writer, batch)

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
SET r.mesma_unidade = e.mesma_unidade,
    r.intervalo_horas = e.intervalo_horas,
    r.intervalo_dias = e.intervalo_dias
MERGE (a2)-[r2:PRECEDIDA_POR]->(a1)
SET r2.mesma_unidade = e.mesma_unidade,
    r2.intervalo_horas = e.intervalo_horas,
    r2.intervalo_dias = e.intervalo_dias
"""

# INICIOU_PROCESSO and SEGUIDO_INDEPENDENTEMENTE_POR are clean
# MATCH+MATCH+MERGE edges — handled by writer.write_edges in build_timeline.


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
    def _parse_dt(s: str) -> datetime:
        try:
            return datetime.fromisoformat(s)
        except (ValueError, TypeError):
            return datetime.min

    i = 0
    while i < len(activities):
        if activities[i]["tipo_acao"] != "PROCESSO-RECEBIDO-UNIDADE":
            i += 1
            continue
        recebido_time = _parse_dt(activities[i].get("data_hora", ""))
        j = i + 1
        while j < len(activities):
            candidate = activities[j]
            cand_time = _parse_dt(candidate.get("data_hora", ""))
            # Past 60s window — stop looking
            if (cand_time - recebido_time).total_seconds() > 60:
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


def _compute_interval(from_atv: dict, to_atv: dict) -> tuple[float | None, float | None]:
    """Compute time interval between two activities in hours and days.
    Returns (intervalo_horas, intervalo_dias) or (None, None) if missing data.
    """
    from_dt = from_atv.get("data_hora", "")
    to_dt = to_atv.get("data_hora", "")
    if not from_dt or not to_dt:
        return None, None
    try:
        delta = datetime.fromisoformat(to_dt) - datetime.fromisoformat(from_dt)
        total_seconds = delta.total_seconds()
        horas = round(total_seconds / 3600, 2)
        dias = round(total_seconds / 86400, 2)
        return horas, dias
    except (ValueError, TypeError):
        return None, None


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
                    h, d = _compute_interval(last_at[src], atv)
                    flow_edges.append({
                        "from_id": last_at[src]["source_id"],
                        "to_id": sid,
                        "mesma_unidade": False,
                        "intervalo_horas": h,
                        "intervalo_dias": d,
                    })
            elif u in last_at:
                # Fallback: connect from last at same unidade
                if last_at[u]["tipo_acao"] not in CONCLUSION_TYPES:
                    h, d = _compute_interval(last_at[u], atv)
                    flow_edges.append({
                        "from_id": last_at[u]["source_id"],
                        "to_id": sid,
                        "mesma_unidade": True,
                        "intervalo_horas": h,
                        "intervalo_dias": d,
                    })
            # Track for RECEBIDO matching
            if u not in pending_remetidos:
                pending_remetidos[u] = []
            pending_remetidos[u].append(atv)

        elif tipo == "PROCESSO-RECEBIDO-UNIDADE":
            # RECEBIDO at unidade u. Connect from pending remetido(s).
            if u in pending_remetidos and pending_remetidos[u]:
                for rem in pending_remetidos[u]:
                    h, d = _compute_interval(rem, atv)
                    flow_edges.append({
                        "from_id": rem["source_id"],
                        "to_id": sid,
                        "mesma_unidade": True,
                        "intervalo_horas": h,
                        "intervalo_dias": d,
                    })
                pending_remetidos[u] = []
            elif u in last_at:
                if last_at[u]["tipo_acao"] not in CONCLUSION_TYPES:
                    h, d = _compute_interval(last_at[u], atv)
                    flow_edges.append({
                        "from_id": last_at[u]["source_id"],
                        "to_id": sid,
                        "mesma_unidade": True,
                        "intervalo_horas": h,
                        "intervalo_dias": d,
                    })

        else:
            # Regular activity: chain to last at same unidade
            # Fix 2: skip if last was a conclusão
            if u in last_at and last_at[u]["tipo_acao"] not in CONCLUSION_TYPES:
                h, d = _compute_interval(last_at[u], atv)
                flow_edges.append({
                    "from_id": last_at[u]["source_id"],
                    "to_id": sid,
                    "mesma_unidade": True,
                    "intervalo_horas": h,
                    "intervalo_dias": d,
                })

        last_at[u] = atv

    return flow_edges, independent_edges


def build_timeline(reader: GraphReader, writer: GraphWriter):
    """Build SEGUIDA_POR/PRECEDIDA_POR DAG + INICIOU_PROCESSO.

    Uses unidade-context tracking instead of naive timestamp grouping.
    Activities chain within the same unidade; transitions happen via
    REMETIDO->RECEBIDO pairs. Parallel branches stay independent.
    """
    log.info("Phase C: Building timeline DAG (unidade-context)...")

    total = reader.count_processos()
    log.info("  Processing %d processos...", total)

    batch_size = 500
    total_edges = 0
    total_inicio = 0
    processed = 0

    for batch in reader.iter_processo_batches(batch_size=batch_size):
        all_flow_edges = []
        all_independent_edges = []
        inicio_rows = []
        for processo in batch:
            pf = processo.protocolo_formatado
            activities = processo.activities
            if not activities:
                continue

            flow_edges, independent_edges = _build_edges_for_processo(activities)
            all_flow_edges.extend(flow_edges)
            all_independent_edges.extend(independent_edges)

            inicio_rows.append({
                "protocolo_formatado": pf,
                "first_id": activities[0]["source_id"],
            })

        # LOAD_TIMELINE is composite (MERGE SEGUIDA_POR + MERGE PRECEDIDA_POR in one
        # statement) — stays as execute_template.
        if all_flow_edges:
            for i in range(0, len(all_flow_edges), 500):
                sub = all_flow_edges[i:i + 500]
                writer.execute_template("load_timeline", LOAD_TIMELINE_CYPHER,
                                        {"edges": sub}, phase="C")
            total_edges += len(all_flow_edges)

        # SEGUIDO_INDEPENDENTEMENTE_POR is a clean edge with one property.
        if all_independent_edges:
            writer.write_edges(
                "SEGUIDO_INDEPENDENTEMENTE_POR",
                "Atividade", {"from_id": "source_id"},
                "Atividade", {"to_id": "source_id"},
                all_independent_edges,
                phase="C",
                props=["ref_id"],
            )
            total_edges += len(all_independent_edges)

        # INICIOU_PROCESSO is a clean edge with no properties.
        if inicio_rows:
            writer.write_edges(
                "INICIOU_PROCESSO",
                "Processo", {"protocolo_formatado": "protocolo_formatado"},
                "Atividade", {"first_id": "source_id"},
                inicio_rows,
                phase="C",
            )
            total_inicio += len(inicio_rows)

        processed += len(batch)
        if processed % 5000 == 0 or processed >= total:
            log.info("  Progress: %d/%d processos, %d edges, %d inicio",
                     min(processed, total), total, total_edges, total_inicio)

    log.info("Phase C complete: %d edges (SEGUIDA_POR + SEGUIDO_INDEPENDENTEMENTE_POR) + %d INICIOU_PROCESSO",
             total_edges, total_inicio)

# ---------------------------------------------------------------------------
# Phase D: Compute permanencia (PASSOU_PELA_UNIDADE + PASSOU_PELO_ORGAO)
# ---------------------------------------------------------------------------
def compute_permanencia(reader: GraphReader, writer: GraphWriter):
    """Compute time each processo spent per unidade and per orgao.

    Uses stint-based grouping: consecutive activities at the same unidade
    form a stint. If a processo visits A->B->A, unidade A gets two stints summed.
    Orgao permanencia is aggregated from unidade stints.
    """
    log.info("Phase D: Computing permanencia (PASSOU_PELA_UNIDADE + PASSOU_PELO_ORGAO)...")

    total = reader.count_processos()
    log.info("  Computing for %d processos...", total)

    batch_size = 1000
    total_unidade_links = 0
    total_orgao_links = 0
    processed = 0

    for batch in reader.iter_processo_batches(batch_size=batch_size):
        unidade_rows = []
        orgao_rows = []
        for processo in batch:
            pf = processo.protocolo_formatado
            # Phase D only needs (data_hora, unidade) from each activity;
            # `activities` already carries that (plus extra unused fields).
            timeline = processo.activities

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

        # PASSOU_PELA_UNIDADE/ORGAO carry datetime() function calls in their SET
        # clauses, so they stay as execute_template.
        if unidade_rows:
            for i in range(0, len(unidade_rows), 500):
                sub = unidade_rows[i:i + 500]
                writer.execute_template("load_permanencia_unidade", LOAD_PERMANENCIA_UNIDADE_CYPHER,
                                        {"rows": sub}, phase="D")
            total_unidade_links += len(unidade_rows)

        if orgao_rows:
            for i in range(0, len(orgao_rows), 500):
                sub = orgao_rows[i:i + 500]
                writer.execute_template("load_permanencia_orgao", LOAD_PERMANENCIA_ORGAO_CYPHER,
                                        {"rows": sub}, phase="D")
            total_orgao_links += len(orgao_rows)

        processed += len(batch)
        if processed % 5000 == 0 or processed >= total:
            log.info("  Progress: %d/%d processos, %d PASSOU_PELA_UNIDADE, %d PASSOU_PELO_ORGAO",
                     min(processed, total), total, total_unidade_links, total_orgao_links)

    log.info("Phase D complete: %d PASSOU_PELA_UNIDADE + %d PASSOU_PELO_ORGAO",
             total_unidade_links, total_orgao_links)


# ---------------------------------------------------------------------------
# --read-json short path
# ---------------------------------------------------------------------------
def _run_read_json_only(args) -> None:
    """Handle the `--read-json DIR` mode: skip Postgres-sourced phases,
    load activities from an emit directory, and run Phase C / Phase D.

    The write destination is:
      - `JsonFileWriter(args.emit_json_dir)` if `--emit-json` was also given
      - `DirectNeo4jWriter(...)` otherwise (requires Neo4j credentials)
    """
    read_dir = _SETTINGS.read_json_dir
    log.info("Reading graph from %s (JSON mode)", read_dir)

    try:
        reader: GraphReader = JsonFileReader(read_dir)
    except Exception as e:
        log.error("%s", e)
        sys.exit(2)

    neo4j_driver = None
    writer: GraphWriter | None = None
    try:
        if _SETTINGS.emit_json_dir is not None:
            log.info("Emitting Phase C/D output to %s", _SETTINGS.emit_json_dir)
            writer = JsonFileWriter(_SETTINGS.emit_json_dir)
        else:
            log.info("Connecting to Neo4j for writes: %s", _SETTINGS.neo4j_uri)
            try:
                neo4j_driver = build_driver(_SETTINGS)
            except ConfigError as e:
                log.error(
                    "%s — --read-json needs either --emit-json DIR or Neo4j "
                    "credentials for the write side.", e,
                )
                sys.exit(2)
            log.info("Neo4j connected")
            writer = DirectNeo4jWriter(neo4j_driver, batch_size=args.batch_size)

        if not args.skip_timeline:
            writer.open_phase("C")
            build_timeline(reader, writer)
            writer.close_phase("C")
        else:
            log.info("Phase C skipped (--skip-timeline)")

        if not args.skip_permanencia:
            writer.open_phase("D")
            compute_permanencia(reader, writer)
            writer.close_phase("D")
        else:
            log.info("Phase D skipped (--skip-permanencia)")

        log.info("Read-json pipeline complete.")
    finally:
        if reader is not None:
            reader.close()
        if writer is not None:
            writer.close()
        if neo4j_driver is not None:
            neo4j_driver.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    global _SETTINGS
    parser = argparse.ArgumentParser(description="ETL: sei_atividades -> Neo4j")
    parser.add_argument("--from", dest="from_date", type=str, help="Filter from date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", type=str, help="Filter to date (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true", help="Classify only, no Neo4j writes")
    parser.add_argument("--batch-size", type=int, default=500, help="Neo4j batch size (default: 500)")
    parser.add_argument("--workers", type=int, default=8, help="Parallel workers (default: 8)")
    parser.add_argument("--chunk-size", type=int, default=200, help="Processos per worker chunk (default: 200)")
    parser.add_argument("--skip-timeline", action="store_true", help="Skip building SEGUIDA_POR DAG")
    parser.add_argument("--skip-permanencia", action="store_true", help="Skip computing permanencia")
    # ETL keeps its own --batch-size / --workers defaults (500 / 8); pipeline
    # supplies the remaining standard flags.
    add_standard_args(parser, skip={"--batch-size", "--workers"})
    args = parser.parse_args()

    _SETTINGS = resolve_settings(args)
    configure_logging(__name__, _SETTINGS.log_level)

    # --read-json: short-circuit path — skip Phase A/B (which require Postgres)
    # and run only Phase C/D against a JsonFileReader loaded from the emit dir.
    if _SETTINGS.read_json_dir is not None:
        _run_read_json_only(args)
        return

    try:
        _SETTINGS.require_postgres()
    except ConfigError as e:
        log.error("%s", e)
        sys.exit(2)

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
        for anc in all_ancestor_unidades(u):
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

    # -- Writer / Neo4j setup --
    #
    # Three modes:
    #   1. --dry-run             : no writer, no driver. Phase B skips writes;
    #                              Phase C/D skipped entirely (always have been).
    #   2. --emit-json DIR       : writer = JsonFileWriter(DIR). Driver is built
    #                              only if Phase C/D are still enabled, since
    #                              those phases currently READ from Neo4j.
    #                              JsonFileReader lands in a later step.
    #   3. default (live Neo4j)  : writer = DirectNeo4jWriter, driver built.
    neo4j_driver = None
    writer: GraphWriter | None = None

    if args.dry_run:
        log.info("Dry run: classify only, no writes")

    elif _SETTINGS.emit_json_dir is not None:
        emit_dir = _SETTINGS.emit_json_dir
        log.info("Emitting graph to NDJSON under %s", emit_dir)
        writer = JsonFileWriter(emit_dir)

        need_reads = not args.skip_timeline or not args.skip_permanencia
        if need_reads:
            log.info("Phase C/D still need to READ from Neo4j (see plan step 10); "
                     "attempting to connect: %s", _SETTINGS.neo4j_uri)
            try:
                neo4j_driver = build_driver(_SETTINGS)
                log.info("Neo4j connected (read-only during emit)")
            except ConfigError as e:
                log.warning(
                    "%s — Phase C and Phase D will be skipped. "
                    "Pass --skip-timeline --skip-permanencia to silence this.",
                    e,
                )
                args.skip_timeline = True
                args.skip_permanencia = True

        writer.open_phase("schema")
        for cypher in SETUP_CONSTRAINTS:
            writer.execute_template("schema_constraint", cypher, {}, phase="schema")
        writer.close_phase("schema")
        log.info("Schema: %d constraints/indexes emitted", len(SETUP_CONSTRAINTS))

        precreate_shared_nodes(writer, processos, all_unidades, user_orgao)

    else:
        log.info("Connecting to Neo4j: %s", _SETTINGS.neo4j_uri)
        try:
            neo4j_driver = build_driver(_SETTINGS)
        except ConfigError as e:
            log.error("%s", e)
            sys.exit(2)
        log.info("Neo4j connected")

        writer = DirectNeo4jWriter(neo4j_driver, batch_size=args.batch_size)

        writer.open_phase("schema")
        for cypher in SETUP_CONSTRAINTS:
            writer.execute_template("schema_constraint", cypher, {}, phase="schema")
        writer.close_phase("schema")
        log.info("Schema: %d constraints/indexes", len(SETUP_CONSTRAINTS))

        precreate_shared_nodes(writer, processos, all_unidades, user_orgao)

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
            pool.submit(process_chunk, chunk, writer, args.dry_run, args.batch_size): i
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

    # -- Reader for Phase C / D (still live Neo4j until step 10) --
    reader: GraphReader | None = Neo4jReader(neo4j_driver) if neo4j_driver is not None else None

    # -- Phase C: Build timeline --
    if reader and writer and not args.dry_run and not args.skip_timeline:
        writer.open_phase("C")
        build_timeline(reader, writer)
        writer.close_phase("C")

    # -- Phase D: Compute permanencia --
    if reader and writer and not args.dry_run and not args.skip_permanencia:
        writer.open_phase("D")
        compute_permanencia(reader, writer)
        writer.close_phase("D")

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

    if reader is not None:
        reader.close()
    if writer is not None:
        writer.close()
    if neo4j_driver is not None:
        neo4j_driver.close()


if __name__ == "__main__":
    main()
