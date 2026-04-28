"""
Statistical summary of processos stored in Neo4j.

Sections:
  1. Overview (total processos, atividades, documentos, unidades, orgãos, usuários)
  2. Date ranges (criação dos processos, último andamento)
  3. Tipos de procedimento (ranking)
  4. Orgãos de criação (ranking)
  5. Unidades mais visitadas (PASSOU_PELA_UNIDADE)
  6. Unidades que mais executaram ações (EXECUTADO_PELA_UNIDADE)
  7. Usuários mais ativos
  8. Distribuição de documentos por processo
  9. Distribuição de andamentos por processo
  10. Tipos de ação mais frequentes
  11. Grupos de atividade
  12. Processos mais longos (permanência total)
  13. Processos parados há mais tempo (último andamento mais antigo)

Usage:
    python scripts/pipeline/ops/resumo_neo4j.py
    python scripts/pipeline/ops/resumo_neo4j.py --orgao "SEAD-PI"
    python scripts/pipeline/ops/resumo_neo4j.py --orgao "SEAD-PI" --top 20
    python scripts/pipeline/ops/resumo_neo4j.py --neo4j-uri bolt://remote:7687
"""

import argparse
import sys
from datetime import datetime, timedelta
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

# ── Helpers ────────────────────────────────────────────────────────────────

def _sec(title):
    print(f"\n{'─' * 70}")
    print(f"  {title}")
    print(f"{'─' * 70}")


def _bar(value, max_val, width=30):
    if max_val == 0:
        return ""
    filled = int(value / max_val * width)
    return "█" * filled + "░" * (width - filled)


def _fmt_hours(h):
    if h is None:
        return "—"
    if h < 24:
        return f"{h:.1f}h"
    days = h / 24
    if days < 365:
        return f"{days:.1f}d"
    return f"{days / 365:.1f}a"


def _fmt_date(d):
    if d is None:
        return "—"
    if isinstance(d, str):
        return d[:19]
    if isinstance(d, datetime):
        return d.strftime("%Y-%m-%d %H:%M")
    return str(d)[:19]


def _pct(part, total):
    if total == 0:
        return "—"
    return f"{part / total * 100:.1f}%"


# ── Orgão filter clause builder ───────────────────────────────────────────

def _orgao_match(alias="p", orgao=None):
    """Returns a Cypher MATCH/WHERE clause to filter processos by orgão de criação."""
    if orgao:
        return f"MATCH ({alias}:Processo)-[:CRIADO_NO_ORGAO]->(o:Orgao {{sigla: $orgao}})"
    return f"MATCH ({alias}:Processo)"


# ── Queries ────────────────────────────────────────────────────────────────

def run_summary(driver, orgao: str | None, top: int):
    with driver.session() as s:
        params = {"orgao": orgao} if orgao else {}
        scope = f" (orgão: {orgao})" if orgao else " (todos)"

        # ════════════════════════════════════════════════════════════════
        # 1. OVERVIEW
        # ════════════════════════════════════════════════════════════════
        _sec(f"VISÃO GERAL{scope}")

        q = f"""
        {_orgao_match('p', orgao)}
        WITH count(p) AS processos
        MATCH (a:Atividade) WITH processos, count(a) AS atividades_total
        MATCH (d:Documento) WITH processos, atividades_total, count(d) AS docs_total
        MATCH (u:Unidade) WITH processos, atividades_total, docs_total, count(u) AS unidades
        MATCH (o:Orgao) WITH processos, atividades_total, docs_total, unidades, count(o) AS orgaos
        MATCH (usr:Usuario) WITH processos, atividades_total, docs_total, unidades, orgaos, count(usr) AS usuarios
        RETURN processos, atividades_total, docs_total, unidades, orgaos, usuarios
        """
        r = s.run(q, **params).single()
        n_processos = r["processos"]
        print(f"  Processos:    {r['processos']:,}")
        print(f"  Atividades:   {r['atividades_total']:,}")
        print(f"  Documentos:   {r['docs_total']:,}")
        print(f"  Unidades:     {r['unidades']:,}")
        print(f"  Órgãos:       {r['orgaos']:,}")
        print(f"  Usuários:     {r['usuarios']:,}")

        if n_processos == 0:
            print("\n  Nenhum processo encontrado.")
            return

        # ════════════════════════════════════════════════════════════════
        # 2. DATE RANGES — Criação dos processos
        # ════════════════════════════════════════════════════════════════
        _sec("DATAS DE CRIAÇÃO DOS PROCESSOS")

        q = f"""
        {_orgao_match('p', orgao)}
        WHERE p.data_criacao IS NOT NULL
        RETURN min(p.data_criacao) AS primeiro,
               max(p.data_criacao) AS ultimo,
               count(p) AS total
        """
        r = s.run(q, **params).single()
        print(f"  Primeiro processo:  {_fmt_date(r['primeiro'])}")
        print(f"  Último processo:    {_fmt_date(r['ultimo'])}")
        print(f"  Com data:           {r['total']:,} / {n_processos:,}")

        # Per-month distribution
        q = f"""
        {_orgao_match('p', orgao)}
        WHERE p.data_criacao IS NOT NULL
        WITH p, substring(toString(p.data_criacao), 0, 7) AS mes
        RETURN mes, count(p) AS total
        ORDER BY mes
        """
        records = list(s.run(q, **params))
        if records:
            max_count = max(r["total"] for r in records)
            print(f"\n  Processos por mês:")
            for r in records:
                print(f"    {r['mes']}  {r['total']:>6,}  {_bar(r['total'], max_count, 25)}")

        # ════════════════════════════════════════════════════════════════
        # 3. DATE RANGES — Último andamento
        # ════════════════════════════════════════════════════════════════
        _sec("ÚLTIMO ANDAMENTO POR PROCESSO")

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
        WITH p, max(a.data_hora) AS ultimo_andamento
        RETURN min(ultimo_andamento) AS mais_antigo,
               max(ultimo_andamento) AS mais_recente,
               count(p) AS total
        """
        r = s.run(q, **params).single()
        print(f"  Último andamento mais antigo:   {_fmt_date(r['mais_antigo'])}")
        print(f"  Último andamento mais recente:  {_fmt_date(r['mais_recente'])}")

        # Distribution: last activity age
        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
        WITH p, max(a.data_hora) AS ultimo
        WITH p, duration.between(ultimo, datetime()).days AS dias_parado
        RETURN
            count(CASE WHEN dias_parado <= 7 THEN 1 END) AS ate_7d,
            count(CASE WHEN dias_parado > 7 AND dias_parado <= 30 THEN 1 END) AS ate_30d,
            count(CASE WHEN dias_parado > 30 AND dias_parado <= 90 THEN 1 END) AS ate_90d,
            count(CASE WHEN dias_parado > 90 AND dias_parado <= 180 THEN 1 END) AS ate_180d,
            count(CASE WHEN dias_parado > 180 AND dias_parado <= 365 THEN 1 END) AS ate_1a,
            count(CASE WHEN dias_parado > 365 THEN 1 END) AS mais_1a
        """
        r = s.run(q, **params).single()
        buckets = [
            ("≤ 7 dias",     r["ate_7d"]),
            ("8–30 dias",    r["ate_30d"]),
            ("31–90 dias",   r["ate_90d"]),
            ("91–180 dias",  r["ate_180d"]),
            ("181–365 dias", r["ate_1a"]),
            ("> 1 ano",      r["mais_1a"]),
        ]
        max_b = max(v for _, v in buckets) if buckets else 1
        print(f"\n  Idade do último andamento:")
        for label, val in buckets:
            print(f"    {label:<15} {val:>6,}  {_bar(val, max_b, 25)}")

        # ════════════════════════════════════════════════════════════════
        # 4. TIPOS DE PROCEDIMENTO
        # ════════════════════════════════════════════════════════════════
        _sec("TIPOS DE PROCEDIMENTO (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (p)-[:TEM_TIPO]->(tp:TipoProcedimento)
        RETURN tp.nome AS tipo, count(p) AS total
        ORDER BY total DESC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            max_count = records[0]["total"]
            for r in records:
                print(f"    {r['total']:>6,}  {_bar(r['total'], max_count, 20)}  {r['tipo']}")

        # ════════════════════════════════════════════════════════════════
        # 5. ÓRGÃOS DE CRIAÇÃO
        # ════════════════════════════════════════════════════════════════
        _sec("ÓRGÃOS QUE MAIS CRIARAM PROCESSOS (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (p)-[:CRIADO_NO_ORGAO]->(o2:Orgao)
        RETURN o2.sigla AS orgao_sigla, count(p) AS total
        ORDER BY total DESC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            max_count = records[0]["total"]
            for r in records:
                print(f"    {r['total']:>6,}  {_bar(r['total'], max_count, 20)}  {r['orgao_sigla']}")

        # ════════════════════════════════════════════════════════════════
        # 6. UNIDADES MAIS VISITADAS (PASSOU_PELA_UNIDADE)
        # ════════════════════════════════════════════════════════════════
        _sec("UNIDADES MAIS VISITADAS (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (p)-[r:PASSOU_PELA_UNIDADE]->(u:Unidade)
        RETURN u.sigla AS unidade,
               count(p) AS processos,
               sum(r.visitas) AS total_visitas,
               avg(r.duracao_total_horas) AS avg_permanencia_h
        ORDER BY processos DESC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            max_count = records[0]["processos"]
            print(f"    {'Processos':>10} {'Visitas':>10} {'Perm. média':>12}  Unidade")
            print(f"    {'─' * 10} {'─' * 10} {'─' * 12}  {'─' * 35}")
            for r in records:
                print(f"    {r['processos']:>10,} {r['total_visitas']:>10,} {_fmt_hours(r['avg_permanencia_h']):>12}  {r['unidade']}")

        # ════════════════════════════════════════════════════════════════
        # 7. UNIDADES QUE MAIS EXECUTARAM AÇÕES
        # ════════════════════════════════════════════════════════════════
        _sec("UNIDADES QUE MAIS EXECUTARAM AÇÕES (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
        MATCH (a)-[:EXECUTADO_PELA_UNIDADE]->(u:Unidade)
        RETURN u.sigla AS unidade, count(a) AS acoes
        ORDER BY acoes DESC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            max_count = records[0]["acoes"]
            for r in records:
                print(f"    {r['acoes']:>8,}  {_bar(r['acoes'], max_count, 20)}  {r['unidade']}")

        # ════════════════════════════════════════════════════════════════
        # 8. USUÁRIOS MAIS ATIVOS
        # ════════════════════════════════════════════════════════════════
        _sec("USUÁRIOS MAIS ATIVOS (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
        MATCH (a)-[:EXECUTADO_PELO_USUARIO]->(usr:Usuario)
        RETURN usr.nome AS usuario, count(a) AS acoes
        ORDER BY acoes DESC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            max_count = records[0]["acoes"]
            for r in records:
                print(f"    {r['acoes']:>8,}  {_bar(r['acoes'], max_count, 20)}  {r['usuario']}")

        # ════════════════════════════════════════════════════════════════
        # 9. TIPOS DE AÇÃO MAIS FREQUENTES
        # ════════════════════════════════════════════════════════════════
        _sec("TIPOS DE AÇÃO MAIS FREQUENTES (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
        MATCH (a)-[:TIPO_ACAO]->(ta:TipoAcao)
        RETURN ta.chave AS tipo_acao, count(a) AS total
        ORDER BY total DESC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            max_count = records[0]["total"]
            for r in records:
                print(f"    {r['total']:>8,}  {_bar(r['total'], max_count, 20)}  {r['tipo_acao']}")

        # ════════════════════════════════════════════════════════════════
        # 10. GRUPOS DE ATIVIDADE
        # ════════════════════════════════════════════════════════════════
        _sec("GRUPOS DE ATIVIDADE")

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
        MATCH (a)-[:TIPO_ACAO]->(ta:TipoAcao)-[:PERTENCE_AO_GRUPO]->(ga:GrupoAtividade)
        RETURN ga.label AS grupo, ga.chave AS chave, count(a) AS total
        ORDER BY total DESC
        """
        records = list(s.run(q, **params))
        if records:
            max_count = records[0]["total"]
            for r in records:
                print(f"    {r['total']:>8,}  {_bar(r['total'], max_count, 20)}  {r['grupo'] or r['chave']}")

        # ════════════════════════════════════════════════════════════════
        # 11. DISTRIBUIÇÃO: ANDAMENTOS POR PROCESSO
        # ════════════════════════════════════════════════════════════════
        _sec("DISTRIBUIÇÃO DE ANDAMENTOS POR PROCESSO")

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
        WITH p, count(a) AS n
        RETURN min(n) AS minimo,
               max(n) AS maximo,
               avg(n) AS media,
               percentileCont(n, 0.5) AS mediana,
               percentileCont(n, 0.9) AS p90,
               percentileCont(n, 0.99) AS p99
        """
        r = s.run(q, **params).single()
        print(f"  Mínimo:   {r['minimo']}")
        print(f"  Máximo:   {r['maximo']}")
        print(f"  Média:    {r['media']:.1f}")
        print(f"  Mediana:  {r['mediana']:.0f}")
        print(f"  P90:      {r['p90']:.0f}")
        print(f"  P99:      {r['p99']:.0f}")

        # ════════════════════════════════════════════════════════════════
        # 12. DISTRIBUIÇÃO: DOCUMENTOS POR PROCESSO
        # ════════════════════════════════════════════════════════════════
        _sec("DISTRIBUIÇÃO DE DOCUMENTOS POR PROCESSO")

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (p)-[:CONTEM_DOCUMENTO]->(d:Documento)
        WITH p, count(d) AS n
        RETURN min(n) AS minimo,
               max(n) AS maximo,
               avg(n) AS media,
               percentileCont(n, 0.5) AS mediana,
               percentileCont(n, 0.9) AS p90,
               percentileCont(n, 0.99) AS p99
        """
        r = s.run(q, **params).single()
        print(f"  Mínimo:   {r['minimo']}")
        print(f"  Máximo:   {r['maximo']}")
        print(f"  Média:    {r['media']:.1f}")
        print(f"  Mediana:  {r['mediana']:.0f}")
        print(f"  P90:      {r['p90']:.0f}")
        print(f"  P99:      {r['p99']:.0f}")

        # ════════════════════════════════════════════════════════════════
        # 13. DISTRIBUIÇÃO: UNIDADES POR PROCESSO
        # ════════════════════════════════════════════════════════════════
        _sec("DISTRIBUIÇÃO DE UNIDADES VISITADAS POR PROCESSO")

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (p)-[:PASSOU_PELA_UNIDADE]->(u:Unidade)
        WITH p, count(u) AS n
        RETURN min(n) AS minimo,
               max(n) AS maximo,
               avg(n) AS media,
               percentileCont(n, 0.5) AS mediana,
               percentileCont(n, 0.9) AS p90
        """
        r = s.run(q, **params).single()
        print(f"  Mínimo:   {r['minimo']}")
        print(f"  Máximo:   {r['maximo']}")
        print(f"  Média:    {r['media']:.1f}")
        print(f"  Mediana:  {r['mediana']:.0f}")
        print(f"  P90:      {r['p90']:.0f}")

        # ════════════════════════════════════════════════════════════════
        # 14. PROCESSOS MAIS LONGOS (permanência total)
        # ════════════════════════════════════════════════════════════════
        _sec("PROCESSOS COM MAIOR PERMANÊNCIA TOTAL (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (p)-[r:PASSOU_PELA_UNIDADE]->(u:Unidade)
        WITH p, sum(r.duracao_total_horas) AS total_horas
        RETURN p.protocolo_formatado AS protocolo, total_horas
        ORDER BY total_horas DESC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            max_h = records[0]["total_horas"] if records[0]["total_horas"] else 1
            for r in records:
                print(f"    {_fmt_hours(r['total_horas']):>10}  {_bar(r['total_horas'] or 0, max_h, 20)}  {r['protocolo']}")

        # ════════════════════════════════════════════════════════════════
        # 15. PROCESSOS PARADOS HÁ MAIS TEMPO
        # ════════════════════════════════════════════════════════════════
        _sec("PROCESSOS PARADOS HÁ MAIS TEMPO (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
        WITH p, max(a.data_hora) AS ultimo
        RETURN p.protocolo_formatado AS protocolo,
               ultimo AS ultimo_andamento,
               duration.between(ultimo, datetime()).days AS dias_parado
        ORDER BY ultimo ASC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            print(f"    {'Dias':>8}  {'Último andamento':<20}  Protocolo")
            print(f"    {'─' * 8}  {'─' * 20}  {'─' * 30}")
            for r in records:
                print(f"    {r['dias_parado']:>8,}  {_fmt_date(r['ultimo_andamento']):<20}  {r['protocolo']}")

        # ════════════════════════════════════════════════════════════════
        # 16. ÓRGÃOS POR PERMANÊNCIA MÉDIA
        # ════════════════════════════════════════════════════════════════
        _sec("ÓRGÃOS COM MAIOR PERMANÊNCIA MÉDIA (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (p)-[r:PASSOU_PELO_ORGAO]->(org:Orgao)
        RETURN org.sigla AS orgao_sigla,
               count(p) AS processos,
               avg(r.duracao_total_horas) AS avg_h,
               max(r.duracao_total_horas) AS max_h
        ORDER BY avg_h DESC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            print(f"    {'Processos':>10} {'Perm. média':>12} {'Perm. máx':>12}  Órgão")
            print(f"    {'─' * 10} {'─' * 12} {'─' * 12}  {'─' * 25}")
            for r in records:
                print(f"    {r['processos']:>10,} {_fmt_hours(r['avg_h']):>12} {_fmt_hours(r['max_h']):>12}  {r['orgao_sigla']}")

        # ════════════════════════════════════════════════════════════════
        # 17. FLUXOS MAIS COMUNS (remetido de → para)
        # ════════════════════════════════════════════════════════════════
        _sec("FLUXOS DE TRAMITAÇÃO MAIS COMUNS (TOP {})".format(top))

        q = f"""
        {_orgao_match('p', orgao)}
        MATCH (a:Atividade)-[:DO_PROCESSO]->(p)
        WHERE a.tipo_acao = 'PROCESSO-REMETIDO-UNIDADE'
        MATCH (a)-[:REMETIDO_PELA_UNIDADE]->(origem:Unidade)
        MATCH (a)-[:EXECUTADO_PELA_UNIDADE]->(destino:Unidade)
        WHERE origem <> destino
        RETURN origem.sigla AS de, destino.sigla AS para, count(*) AS total
        ORDER BY total DESC LIMIT $top
        """
        records = list(s.run(q, top=top, **params))
        if records:
            max_count = records[0]["total"]
            for r in records:
                print(f"    {r['total']:>6,}  {_bar(r['total'], max_count, 15)}  {r['de']}  →  {r['para']}")

        print(f"\n{'=' * 70}\n")


def run_summary_json(emit_dir: _Path, top: int) -> None:
    """Subset of `run_summary` that operates on a JsonFileWriter emit directory.

    Supported sections: counts per node label, counts per edge type, top-N
    distribution of common Atividade.tipo_acao. Anything that requires graph
    traversal (paths, lead time, etc.) raises a clear error pointing the
    user to ``--mode neo4j``.
    """
    from collections import Counter
    import json

    if not emit_dir.is_dir():
        log.error("Emit directory not found: %s", emit_dir)
        sys.exit(1)

    print(f"\n{'=' * 70}")
    print(f"  RESUMO (JSON snapshot — {emit_dir})")
    print(f"{'=' * 70}")

    nodes_dir = emit_dir / "nodes"
    edges_dir = emit_dir / "edges"
    templates_dir = emit_dir / "templates"

    # ── 1. Node counts per label ──
    _sec("1. Counts por label (nodes)")
    if nodes_dir.is_dir():
        node_counts: dict[str, int] = {}
        for f in sorted(nodes_dir.glob("*.ndjson")):
            with f.open("r", encoding="utf-8") as fh:
                node_counts[f.stem] = sum(1 for _ in fh)
        if node_counts:
            mx = max(node_counts.values())
            for label, n in sorted(node_counts.items(), key=lambda x: -x[1]):
                print(f"  {label:30s} {n:>8d}  {_bar(n, mx)}")
        else:
            print("  (nenhum nó emitido)")
    else:
        print("  (diretório nodes/ ausente)")

    # ── 2. Edge counts per relationship ──
    _sec("2. Counts por relationship (edges)")
    if edges_dir.is_dir():
        edge_counts: dict[str, int] = {}
        for f in sorted(edges_dir.glob("*.ndjson")):
            with f.open("r", encoding="utf-8") as fh:
                edge_counts[f.stem] = sum(1 for _ in fh)
        if edge_counts:
            mx = max(edge_counts.values())
            for rel, n in sorted(edge_counts.items(), key=lambda x: -x[1]):
                print(f"  {rel:30s} {n:>8d}  {_bar(n, mx)}")
        else:
            print("  (nenhuma aresta emitida)")
    else:
        print("  (diretório edges/ ausente)")

    # ── 3. Top tipos de ação (parses templates/load_atividades.ndjson) ──
    _sec(f"3. Top {top} tipos de ação (de templates/load_atividades.ndjson)")
    activities_template = templates_dir / "load_atividades.ndjson"
    if activities_template.is_file():
        tipo_counter: Counter = Counter()
        total = 0
        with activities_template.open("r", encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                rows = rec.get("params", {}).get("rows") or []
                for row in rows:
                    tipo = row.get("tipo_acao")
                    if tipo:
                        tipo_counter[tipo] += 1
                        total += 1
        if total:
            mx = tipo_counter.most_common(1)[0][1]
            for tipo, n in tipo_counter.most_common(top):
                print(f"  {tipo:35s} {n:>8d}  ({_pct(n, total)})  {_bar(n, mx)}")
        else:
            print("  (sem atividades no snapshot)")
    else:
        print(f"  (arquivo {activities_template} ausente)")

    # ── 4. Sections inviáveis em JSON ──
    _sec("Não disponível em modo JSON")
    print("  As seções abaixo exigem traversal do grafo e só rodam em --mode neo4j:")
    print("    - permanência por unidade/órgão (PASSOU_PELA_UNIDADE / PASSOU_PELO_ORGAO)")
    print("    - rotas comuns de tramitação")
    print("    - processos parados há mais tempo")
    print("    - usuários/unidades mais ativos por contagem agregada")
    print(f"\n  Use: pipeline run resumo --mode neo4j  (após replay do snapshot)")
    print(f"\n{'=' * 70}\n")


def main():
    parser = argparse.ArgumentParser(description="Statistical summary of the graph (Neo4j or JSON)")
    parser.add_argument("--orgao", default=None,
                        help="Filter only processos created in this orgão (e.g. 'SEAD-PI'). Neo4j mode only.")
    parser.add_argument("--top", type=int, default=15,
                        help="How many items to show in rankings (default: 15)")
    add_standard_args(parser)
    args = parser.parse_args()

    settings = resolve_settings(args)
    configure_logging(__name__, settings.log_level)

    if settings.emit_json_dir is not None:
        run_summary_json(_Path(settings.emit_json_dir), args.top)
        return
    if settings.read_json_dir is not None:
        run_summary_json(_Path(settings.read_json_dir), args.top)
        return

    try:
        driver = build_driver(settings)
        log.info("Connected to Neo4j: %s", settings.neo4j_uri)
    except ConfigError as e:
        log.error("%s", e)
        sys.exit(2)
    except Exception as e:
        log.error("Failed to connect to Neo4j at %s: %s", settings.neo4j_uri, e)
        sys.exit(1)

    scope = f" — orgão: {args.orgao}" if args.orgao else ""
    print(f"\n{'=' * 70}")
    print(f"  RESUMO NEO4J{scope}")
    print(f"{'=' * 70}")

    run_summary(driver, args.orgao, args.top)
    driver.close()


# ---------------------------------------------------------------------------
# Stage entry point
# ---------------------------------------------------------------------------
from pipeline.registry import stage  # noqa: E402
from pipeline._stage_base import RunContext, StageMeta  # noqa: E402


@stage(StageMeta(
    name="resumo",
    description="Sumário estatístico do grafo. Subset disponível em modo JSON.",
    type="op",
    depends_on=(),
    modes=("neo4j", "json-emit", "json-replay"),
    can_skip_when_done=False,  # always runs when invoked
    estimated_duration="<30s",
))
def run(ctx: RunContext) -> None:
    top = int(ctx.flags.get("top") or 15)
    if ctx.mode in ("json-emit", "json-replay"):
        emit_dir = ctx.flags.get("emit_dir") or ctx.flags.get("read_dir")
        if not emit_dir:
            emit_dir = ctx.settings.emit_json_dir or ctx.settings.read_json_dir
        if not emit_dir:
            raise RuntimeError(
                "resumo em modo JSON requer --emit-dir ou --read-dir"
            )
        run_summary_json(_Path(emit_dir), top)
        ctx.cache["resumo_summary"] = {"mode": ctx.mode, "source": str(emit_dir)}
        return

    driver = ctx.require_driver()
    orgao = ctx.flags.get("orgao")
    scope = f" — orgão: {orgao}" if orgao else ""
    print(f"\n{'=' * 70}")
    print(f"  RESUMO NEO4J{scope}")
    print(f"{'=' * 70}")
    run_summary(driver, orgao, top)
    ctx.cache["resumo_summary"] = {"mode": ctx.mode, "orgao": orgao, "top": top}


if __name__ == "__main__":
    main()
