"""Stage ``precreate`` — pre-create shared dimension nodes (was Phase A).

Creates Orgao, Unidade, TipoAcao, GrupoAtividade, TipoProcedimento, Usuario
and their structural edges before parallel atividade loading begins.
Single-threaded to avoid Neo4j MERGE deadlocks on shared nodes.

Modes: ``neo4j``, ``json-emit``. Depends on Postgres for discovery.
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime
from typing import Iterable, Mapping

from ..classification import extract_orgao
from ..classifier import get_all_grupo_records, get_all_tipo_acao_records
from ..cypher import (
    PRECREATE_PROCESSOS_CYPHER,
    PRECREATE_USUARIOS_CYPHER,
    SEED_TIPOS_CYPHER,
)
from ..discovery import discover
from ..hierarchy import parent_unidade
from ..registry import stage
from ..writers import GraphWriter
from .._stage_base import RunContext, StageMeta

log = logging.getLogger(__name__)


def _default_csv_path() -> str:
    """Resolve the default location of `notebooks/cost/unidades_sei.csv`.

    Mirrors the path computed by the legacy monolith — three levels up from
    this file to reach the repo root, then into `notebooks/cost`.
    """
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..", "..", "..", "..",
        "notebooks", "cost", "unidades_sei.csv",
    )


def _load_unidade_csv(path: str) -> dict[str, dict]:
    """Load IdUnidade/Sigla/Descricao lookup from CSV.

    Missing file is logged at WARNING and returns an empty dict so the ETL
    can continue with id_unidade/descricao set to None.
    """
    lookup: dict[str, dict] = {}
    try:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                lookup[row["Sigla"]] = {
                    "id_unidade": row["IdUnidade"],
                    "descricao": row["Descricao"],
                }
        log.info("  Loaded %d unidades from CSV lookup", len(lookup))
    except FileNotFoundError:
        log.warning("  CSV file not found at %s – id_unidade/descricao will be None", path)
    return lookup


def precreate_shared_nodes(
    writer: GraphWriter,
    processos: Iterable[Mapping],
    all_unidades: set[str],
    user_orgao: dict[str, str],
    *,
    csv_path: str | None = None,
) -> None:
    """Create dimension nodes + structural edges for the graph.

    `processos` must be iterable multiple times — pass a materialized list,
    not a cursor. `all_unidades` includes ancestors for SUBUNIDADE_DE
    hierarchy, computed by the caller.
    """
    log.info("Phase A: Pre-creating shared dimension nodes...")
    writer.open_phase("A")

    processos = list(processos)

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

    # Unidade enrichment from CSV
    unidade_csv_lookup = _load_unidade_csv(csv_path or _default_csv_path())
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

    # Processos + TipoProcedimento (composite template)
    proc_rows = []
    for row in processos:
        data_criacao_str = None
        if row["data_hora"]:
            dt = row["data_hora"]
            data_criacao_str = (
                dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt)
            )
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
# Stage entry point
# ---------------------------------------------------------------------------
@stage(StageMeta(
    name="precreate",
    description="Cria nós-dimensão (Orgao, Unidade, TipoAcao, …) antes do load paralelo.",
    type="core",
    depends_on=(),
    soft_depends_on=("unidades",),
    modes=("neo4j", "json-emit"),
    estimated_duration="~1-3min",
))
def run(ctx: RunContext) -> None:
    """Stage runner: pulls discovery from cache (or computes it) and precreates."""
    ctx.settings.require_postgres()
    discovery = ctx.cached(
        "discovery",
        lambda: discover(
            ctx.settings,
            ctx.flags.get("from_date"),
            ctx.flags.get("to_date"),
        ),
    )
    if not discovery["protocolo_ids"]:
        log.info("No processos found — nothing to precreate.")
        ctx.cache["precreate_summary"] = {"processos": 0, "unidades": 0, "usuarios": 0}
        return

    writer = ctx.require_writer()
    csv_path = ctx.flags.get("csv_path") or _default_csv_path()
    precreate_shared_nodes(
        writer,
        discovery["processos"],
        discovery["all_unidades"],
        discovery["user_orgao"],
        csv_path=csv_path,
    )
    ctx.cache["precreate_summary"] = {
        "processos": len(discovery["protocolo_ids"]),
        "unidades": len(discovery["all_unidades"]),
        "usuarios": len(discovery["user_orgao"]),
    }
