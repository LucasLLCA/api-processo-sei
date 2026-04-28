"""Postgres discovery queries shared by Phase A (precreate) and Phase B (atividades).

The original `etl.py` ran these once at the start of the run and threaded
the result through both phases. With the stage-registry architecture each
stage retrieves the same payload via ``ctx.cached('discovery', ...)`` so
the work runs at most once per pipeline invocation.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg2.extras

from .classification import extract_orgao
from .classifier import extract_source_unidade
from .config import Settings
from .hierarchy import all_ancestor_unidades
from .postgres import make_pg_conn
from .sql import (
    FIND_DISTINCT_UNIDADES_SQL,
    FIND_PROCESSOS_SQL,
    FIND_REMETIDO_DESCRICOES_SQL,
    FIND_USUARIOS_SQL,
)

log = logging.getLogger(__name__)


def _build_date_filter(from_date: str | None, to_date: str | None) -> tuple[str, list[str]]:
    if from_date and to_date:
        return "AND data_hora >= %s AND data_hora < %s", [from_date, to_date]
    if from_date:
        return "AND data_hora >= %s", [from_date]
    if to_date:
        return "AND data_hora < %s", [to_date]
    return "", []


def discover(
    settings: Settings,
    from_date: str | None,
    to_date: str | None,
) -> dict[str, Any]:
    """Run discovery queries and return processos + unidades + usuarios."""
    log.info("Discovery: Finding processos (creation events)...")

    date_filter, params = _build_date_filter(from_date, to_date)

    pg_conn = make_pg_conn(settings)
    try:
        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        final_sql = FIND_PROCESSOS_SQL.format(date_filter=date_filter)
        log.info("SQL: %s", final_sql.strip()[:200])
        log.info("Params: %s", params)
        cursor.execute(final_sql, params)
        processos = [dict(row) for row in cursor.fetchall()]
        cursor.close()
    finally:
        pg_conn.close()

    protocolo_ids = [row["protocolo_formatado"] for row in processos]
    log.info("Found %d processos", len(protocolo_ids))

    if not protocolo_ids:
        return {
            "processos": [],
            "protocolo_ids": [],
            "all_unidades": set(),
            "user_orgao": {},
        }

    log.info("Scanning andamentos for unidades and usuarios...")
    all_unidades: set[str] = {"DESCONHECIDA"}
    for row in processos:
        if row["unidade"]:
            all_unidades.add(row["unidade"])

    pg_conn = make_pg_conn(settings)
    try:
        cursor = pg_conn.cursor()
        cursor.execute(FIND_DISTINCT_UNIDADES_SQL, (protocolo_ids,))
        for (u,) in cursor:
            all_unidades.add(u)

        cursor.execute(FIND_REMETIDO_DESCRICOES_SQL, (protocolo_ids,))
        for (desc,) in cursor:
            src = extract_source_unidade(desc)
            if src:
                all_unidades.add(src)

        log.info("Found %d unique unidades", len(all_unidades))

        ancestors: set[str] = set()
        for u in list(all_unidades):
            for anc in all_ancestor_unidades(u):
                ancestors.add(anc)
        added = ancestors - all_unidades
        all_unidades |= ancestors
        if added:
            log.info("  Added %d ancestor unidades for hierarchy", len(added))

        cursor = pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(FIND_USUARIOS_SQL, (protocolo_ids,))
        user_orgao: dict[str, str] = {"DESCONHECIDO": "DESCONHECIDO"}
        for row in cursor:
            usuario = row["usuario"]
            unidade = row["unidade"]
            if usuario not in user_orgao:
                user_orgao[usuario] = extract_orgao(unidade)
        cursor.close()
    finally:
        pg_conn.close()
    log.info("Found %d unique usuarios", len(user_orgao))

    return {
        "processos": processos,
        "protocolo_ids": protocolo_ids,
        "all_unidades": all_unidades,
        "user_orgao": user_orgao,
    }
