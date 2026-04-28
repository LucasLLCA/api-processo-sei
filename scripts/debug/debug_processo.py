"""
Debug a specific processo in Neo4j to inspect its graph state.

Checks:
  1. Which unidade created the processo
  2. Which unidades it passed through (with permanência stats)
  3. Whether any unidade currently has it open (não concluído)
  4. How many andamentos (atividades) it has
  5. How many documentos it has
  6. Which unidade created a specific document (if --documento is provided)

Usage:
    python scripts/debug/debug_processo.py "00002.000175/2025-63"
    python scripts/debug/debug_processo.py "00002.000175/2025-63" --documento "1234567"
    python scripts/debug/debug_processo.py "00002.000175/2025-63" --neo4j-uri bolt://remote:7687
"""

import argparse
import sys
from pathlib import Path as _Path

_HERE = _Path(__file__).resolve()
_SCRIPTS = next(p for p in _HERE.parents if p.name == "scripts")
for _p in (_SCRIPTS, _SCRIPTS.parent):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from pipeline.cli import add_standard_args, resolve_settings
from pipeline.config import ConfigError
from pipeline.logging_setup import configure_logging
from pipeline.neo4j_driver import build_driver

log = configure_logging(__name__)

# ── Queries ────────────────────────────────────────────────────────────────

Q_PROCESSO_BASIC = """
MATCH (p:Processo {protocolo_formatado: $protocolo})
OPTIONAL MATCH (p)-[:TEM_TIPO]->(tp:TipoProcedimento)
OPTIONAL MATCH (p)-[:CRIADO_NA_UNIDADE]->(cu:Unidade)
OPTIONAL MATCH (p)-[:CRIADO_NO_ORGAO]->(co:Orgao)
RETURN p.protocolo_formatado AS protocolo,
       p.data_criacao AS data_criacao,
       tp.nome AS tipo_procedimento,
       cu.sigla AS unidade_criacao,
       co.sigla AS orgao_criacao
"""

Q_UNIDADES_PASSARAM = """
MATCH (p:Processo {protocolo_formatado: $protocolo})-[r:PASSOU_PELA_UNIDADE]->(u:Unidade)
RETURN u.sigla AS sigla,
       u.id_unidade AS id_unidade,
       r.duracao_total_horas AS horas,
       r.visitas AS visitas
ORDER BY r.duracao_total_horas DESC
"""

Q_ORGAOS_PASSARAM = """
MATCH (p:Processo {protocolo_formatado: $protocolo})-[r:PASSOU_PELO_ORGAO]->(o:Orgao)
RETURN o.sigla AS sigla,
       r.duracao_total_horas AS horas,
       r.visitas AS visitas
ORDER BY r.duracao_total_horas DESC
"""

Q_UNIDADE_ABERTA = """
MATCH (p:Processo {protocolo_formatado: $protocolo})-[:DO_PROCESSO]-(a:Atividade)
WHERE a.tipo_acao IN ['PROCESSO-RECEBIDO-UNIDADE', 'REABERTURA-PROCESSO-UNIDADE']
WITH a
ORDER BY a.data_hora DESC
LIMIT 1
OPTIONAL MATCH (a)-[:EXECUTADO_PELA_UNIDADE]->(u:Unidade)
RETURN a.tipo_acao AS ultima_acao,
       a.data_hora AS data_hora,
       u.sigla AS unidade
"""

Q_ULTIMA_ATIVIDADE = """
MATCH (p:Processo {protocolo_formatado: $protocolo})-[:DO_PROCESSO]-(a:Atividade)
WITH a ORDER BY a.data_hora DESC LIMIT 1
OPTIONAL MATCH (a)-[:EXECUTADO_PELA_UNIDADE]->(u:Unidade)
RETURN a.tipo_acao AS tipo_acao,
       a.descricao AS descricao,
       a.data_hora AS data_hora,
       u.sigla AS unidade
"""

Q_COUNT_ATIVIDADES = """
MATCH (p:Processo {protocolo_formatado: $protocolo})-[:DO_PROCESSO]-(a:Atividade)
RETURN count(a) AS total
"""

Q_COUNT_DOCUMENTOS = """
MATCH (p:Processo {protocolo_formatado: $protocolo})-[:CONTEM_DOCUMENTO]->(d:Documento)
RETURN count(d) AS total
"""

Q_LIST_DOCUMENTOS = """
MATCH (p:Processo {protocolo_formatado: $protocolo})-[:CONTEM_DOCUMENTO]->(d:Documento)
RETURN d.numero AS numero, d.tipo AS tipo
ORDER BY d.numero
"""

Q_DOCUMENTO_CRIADOR = """
MATCH (a:Atividade)-[:REFERENCIA_DOCUMENTO]->(d:Documento {numero: $documento})
WHERE a.tipo_acao IN ['GERACAO-DOCUMENTO', 'ARQUIVO-ANEXADO', 'RECEBIMENTO-DOCUMENTO']
OPTIONAL MATCH (a)-[:EXECUTADO_PELA_UNIDADE]->(u:Unidade)
OPTIONAL MATCH (a)-[:EXECUTADO_PELO_USUARIO]->(usr:Usuario)
RETURN a.tipo_acao AS acao,
       a.data_hora AS data_hora,
       u.sigla AS unidade,
       u.id_unidade AS id_unidade,
       usr.nome AS usuario
ORDER BY a.data_hora
LIMIT 1
"""

Q_DOCUMENTO_ATIVIDADES = """
MATCH (a:Atividade)-[:REFERENCIA_DOCUMENTO]->(d:Documento {numero: $documento})
OPTIONAL MATCH (a)-[:EXECUTADO_PELA_UNIDADE]->(u:Unidade)
OPTIONAL MATCH (a)-[:EXECUTADO_PELO_USUARIO]->(usr:Usuario)
RETURN a.tipo_acao AS acao,
       a.data_hora AS data_hora,
       a.descricao AS descricao,
       u.sigla AS unidade,
       usr.nome AS usuario
ORDER BY a.data_hora
"""


# ── Helpers ────────────────────────────────────────────────────────────────

def fmt_hours(h):
    if h is None:
        return "?"
    if h < 24:
        return f"{h:.1f}h"
    return f"{h / 24:.1f}d ({h:.0f}h)"


def print_section(title):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── Main ───────────────────────────────────────────────────────────────────

def debug_processo(driver, protocolo: str, documento: str | None = None):
    with driver.session() as session:

        # 1. Basic info
        print_section("PROCESSO")
        rec = session.run(Q_PROCESSO_BASIC, protocolo=protocolo).single()
        if rec is None:
            print(f"  Processo '{protocolo}' NOT FOUND in Neo4j.")
            return
        print(f"  Protocolo:        {rec['protocolo']}")
        print(f"  Data criacao:     {rec['data_criacao']}")
        print(f"  Tipo procedimento:{rec['tipo_procedimento']}")
        print(f"  Unidade criacao:  {rec['unidade_criacao']}")
        print(f"  Orgao criacao:    {rec['orgao_criacao']}")

        # 2. Counts
        print_section("CONTAGENS")
        n_ativ = session.run(Q_COUNT_ATIVIDADES, protocolo=protocolo).single()["total"]
        n_docs = session.run(Q_COUNT_DOCUMENTOS, protocolo=protocolo).single()["total"]
        print(f"  Andamentos (atividades): {n_ativ}")
        print(f"  Documentos:              {n_docs}")

        # 3. Unidades that it passed through
        print_section("UNIDADES QUE PASSOU")
        records = list(session.run(Q_UNIDADES_PASSARAM, protocolo=protocolo))
        if not records:
            print("  Nenhuma relacao PASSOU_PELA_UNIDADE encontrada.")
        else:
            print(f"  {'Unidade':<40} {'id_unidade':>12} {'Permanencia':>14} {'Visitas':>8}")
            print(f"  {'─' * 40} {'─' * 12} {'─' * 14} {'─' * 8}")
            for r in records:
                print(f"  {r['sigla'] or '?':<40} {r['id_unidade'] or '?':>12} {fmt_hours(r['horas']):>14} {r['visitas'] or '?':>8}")

        # 4. Orgaos
        print_section("ORGAOS QUE PASSOU")
        records = list(session.run(Q_ORGAOS_PASSARAM, protocolo=protocolo))
        if not records:
            print("  Nenhuma relacao PASSOU_PELO_ORGAO encontrada.")
        else:
            for r in records:
                print(f"  {r['sigla']:<30} permanencia: {fmt_hours(r['horas'])}  visitas: {r['visitas']}")

        # 5. Current state — which unidade has it open?
        print_section("ESTADO ATUAL")
        rec = session.run(Q_UNIDADE_ABERTA, protocolo=protocolo).single()
        if rec and rec["ultima_acao"]:
            action = rec["ultima_acao"]
            is_open = action in ("PROCESSO-RECEBIDO-UNIDADE", "REABERTURA-PROCESSO-UNIDADE")
            status = "ABERTO" if is_open else "CONCLUIDO/REMETIDO"
            print(f"  Ultima acao de tramitacao: {action}")
            print(f"  Unidade:                  {rec['unidade']}")
            print(f"  Data:                     {rec['data_hora']}")
            print(f"  Status inferido:          {status}")
        else:
            print("  Nenhuma atividade de tramitacao encontrada.")

        rec = session.run(Q_ULTIMA_ATIVIDADE, protocolo=protocolo).single()
        if rec and rec["tipo_acao"]:
            print(f"\n  Ultima atividade geral:")
            print(f"    Tipo:     {rec['tipo_acao']}")
            print(f"    Unidade:  {rec['unidade']}")
            print(f"    Data:     {rec['data_hora']}")
            if rec["descricao"]:
                print(f"    Descricao:{rec['descricao'][:120]}")

        # 6. List documents
        if n_docs > 0:
            print_section(f"DOCUMENTOS ({n_docs})")
            records = list(session.run(Q_LIST_DOCUMENTOS, protocolo=protocolo))
            for r in records:
                print(f"  {r['numero']:<15} tipo: {r['tipo'] or '?'}")

        # 7. Debug specific document
        if documento:
            print_section(f"DOCUMENTO {documento} — DETALHES")
            rec = session.run(Q_DOCUMENTO_CRIADOR, documento=documento).single()
            if rec and rec["acao"]:
                print(f"  Criado por:")
                print(f"    Acao:       {rec['acao']}")
                print(f"    Unidade:    {rec['unidade']} (id={rec['id_unidade']})")
                print(f"    Usuario:    {rec['usuario']}")
                print(f"    Data:       {rec['data_hora']}")
            else:
                print(f"  Documento '{documento}' nao encontrado ou sem atividade de criacao.")

            print(f"\n  Todas as atividades referenciando este documento:")
            records = list(session.run(Q_DOCUMENTO_ATIVIDADES, documento=documento))
            if not records:
                print("    Nenhuma.")
            else:
                for r in records:
                    print(f"    [{r['data_hora']}] {r['acao']:<35} unidade: {r['unidade'] or '?':<30} usuario: {r['usuario'] or '?'}")


def main():
    parser = argparse.ArgumentParser(
        description="Debug a processo in Neo4j — inspect graph state, unidades, documents"
    )
    parser.add_argument("protocolo", help="Protocolo formatado do processo (e.g. '00002.000175/2025-63')")
    parser.add_argument("--documento", default=None, help="Specific documento numero to inspect")
    add_standard_args(parser)
    args = parser.parse_args()

    settings = resolve_settings(args)
    configure_logging(__name__, settings.log_level)

    try:
        driver = build_driver(settings)
        log.info("Connected to Neo4j: %s", settings.neo4j_uri)
    except ConfigError as e:
        log.error("%s", e)
        sys.exit(2)
    except Exception as e:
        log.error("Failed to connect to Neo4j at %s: %s", settings.neo4j_uri, e)
        sys.exit(1)

    print(f"\n{'=' * 60}")
    print(f"  DEBUG PROCESSO: {args.protocolo}")
    print(f"{'=' * 60}")

    debug_processo(driver, args.protocolo, args.documento)

    print(f"\n{'=' * 60}\n")
    driver.close()


if __name__ == "__main__":
    main()
