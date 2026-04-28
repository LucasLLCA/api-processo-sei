"""Pure-Python algorithm: derive a processo's state from its activity timeline.

Single source of truth for "is unit X open?", "is the processo concluded?",
"how long did each cycle take?". Used by the ``permanencia`` and ``situacao``
stages — they share the same compute via ``ctx.cached``.

**Semântica inbox-SEI** (espelha o que o usuário vê em "Processos recebidos"
da UI). Mesma regra usada em ``api-sei-atividaes/app/models/estoque_rules.py``,
``api-sei-atividaes/app/tasks/estoque_processos.py`` e
``studio/src/lib/process-flow-utils.ts``.

The algorithm walks each unit's activities chronologically and detects
**cycles** (entrada → conclusão pairs):

- ACTIVATION events open a new cycle on the unit:
    GERACAO-PROCEDIMENTO        (unidade criou o processo)
    PROCESSO-REMETIDO-UNIDADE   (chegou na inbox da unidade — destino)
    REABERTURA-PROCESSO-UNIDADE (ciclo reaberto pela própria unidade)

- CONCLUSION events close the open cycle on the unit:
    CONCLUSAO-PROCESSO-UNIDADE, CONCLUSAO-AUTOMATICA-UNIDADE.

- PROCESSO-RECEBIDO-UNIDADE é tratado como **TIE-BREAK ONLY**, não abre nem
  fecha. O SEI ocasionalmente registra RECEBIDO antes do REMETIDO
  correspondente (clock skew em segundos/ms). O sort em
  ``timeline.sort_and_fix_activities`` aplica priority(REMETIDO=0,
  RECEBIDO=1, CONCLUSAO=2) pra resolver isso.

- All other activity types update the unit's "last significant" pointer
  but do not open/close cycles.

A processo é ``concluido`` iff **toda** unidade que tocou tem seu último
ciclo fechado. Uma unidade é ``em_aberto`` iff seu último ciclo está aberto
(sem CONCLUSAO após a última ACTIVATION).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .classification import extract_orgao
from .timeline import sort_and_fix_activities


# ---------------------------------------------------------------------------
# Activity classification — semântica inbox-SEI (canônica, alinhada com
# api-sei-atividaes/app/models/estoque_rules.py)
# ---------------------------------------------------------------------------
ACTIVATION_TYPES: frozenset[str] = frozenset({
    "GERACAO-PROCEDIMENTO",
    "PROCESSO-REMETIDO-UNIDADE",
    "REABERTURA-PROCESSO-UNIDADE",
})

CONCLUSION_TYPES: frozenset[str] = frozenset({
    "CONCLUSAO-PROCESSO-UNIDADE",
    "CONCLUSAO-AUTOMATICA-UNIDADE",
})

# Significant for the "last significant action" pointer.
# RECEBIDO entra aqui só pra que o sort/walk preserve a sequência natural;
# ele NÃO está em ACTIVATION_TYPES (não abre ciclo).
SIGNIFICANT_TYPES: frozenset[str] = frozenset(
    ACTIVATION_TYPES | CONCLUSION_TYPES | {"PROCESSO-RECEBIDO-UNIDADE"}
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _parse_dt(s: str | None) -> datetime | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _hours_between(a: str | None, b: str | None) -> float:
    """Return (b - a) in hours, or 0.0 if either is missing/invalid."""
    da = _parse_dt(a)
    db = _parse_dt(b)
    if da is None or db is None:
        return 0.0
    return (db - da).total_seconds() / 3600.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def compute_processo_state(
    activities: list[dict[str, Any]],
    *,
    protocolo_formatado: str | None = None,
    now_iso: str | None = None,
) -> dict[str, Any]:
    """Derive cycles + situação for one processo from its activities.

    ``activities`` is a list of dicts with at least:
        source_id, data_hora, tipo_acao, unidade.
    They will be re-sorted internally by ``sort_and_fix_activities`` so the
    caller doesn't need to pre-sort.

    ``protocolo_formatado`` is folded into ciclo IDs to keep them globally
    unique. If omitted, falls back to ``"unknown"`` — useful for unit tests.

    ``now_iso`` lets the caller pin a deterministic "now" for snapshot
    fields (``dias_sem_atividade``, ``situacao_computed_at``). Defaults to
    ``datetime.now(UTC).isoformat()`` at call time.
    """
    if not activities:
        return _empty_state(protocolo_formatado, now_iso or _now_iso())

    pf = protocolo_formatado or activities[0].get("protocolo_formatado") or "unknown"
    now = now_iso or _now_iso()
    now_dt = _parse_dt(now) or datetime.now(timezone.utc)

    # Mutate-safe copy + canonical ordering
    sorted_activities = sort_and_fix_activities(list(activities))

    data_inicio = sorted_activities[0]["data_hora"]
    data_ultima_atividade = sorted_activities[-1]["data_hora"]
    atividade_mais_recente_id = sorted_activities[-1]["source_id"]

    # ── Per-unit walk: build cycles + last-significant pointer ──
    units = _walk_units(sorted_activities, pf)

    # ── Per-orgao aggregation ──
    orgaos = _aggregate_orgaos(units, data_ultima_atividade)

    # ── Process-level rollup ──
    abertas_siglas = sorted(u["unidade"] for u in units if u["situacao"] == "em_aberto")
    concluido = len(abertas_siglas) == 0

    # data_conclusao_global = max(data_hora) entre todas as CONCLUSION
    data_conclusao_global: str | None = None
    if concluido:
        for atv in reversed(sorted_activities):
            if atv["tipo_acao"] in CONCLUSION_TYPES:
                data_conclusao_global = atv["data_hora"]
                break

    duracao_lifetime_horas = (
        _hours_between(data_inicio, data_conclusao_global) if concluido else None
    )

    # Snapshot dias_sem_atividade per-unit + per-orgao using `now`
    for u in units:
        u["dias_sem_atividade"] = _days_since(u["ultima_atividade_data_hora"], now_dt)
    for o in orgaos:
        o["dias_sem_atividade"] = _days_since(o["ultima_atividade_data_hora"], now_dt)

    return {
        "processo": {
            "protocolo_formatado": pf,
            "situacao": "concluido" if concluido else "em_andamento",
            "unidades_em_aberto": abertas_siglas,
            "unidades_em_aberto_count": len(abertas_siglas),
            "data_inicio": data_inicio,
            "data_ultima_atividade": data_ultima_atividade,
            "data_conclusao_global": data_conclusao_global,
            "duracao_lifetime_horas": duracao_lifetime_horas,
            "duracao_lifetime_dias": (
                round(duracao_lifetime_horas / 24.0, 2)
                if duracao_lifetime_horas is not None else None
            ),
            "atividade_mais_recente_id": atividade_mais_recente_id,
            "situacao_computed_at": now,
        },
        "unidades": units,
        "orgaos": orgaos,
    }


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------
def _empty_state(pf: str | None, now: str) -> dict[str, Any]:
    return {
        "processo": {
            "protocolo_formatado": pf or "unknown",
            "situacao": "em_andamento",  # ill-defined; default conservative
            "unidades_em_aberto": [],
            "unidades_em_aberto_count": 0,
            "data_inicio": None,
            "data_ultima_atividade": None,
            "data_conclusao_global": None,
            "duracao_lifetime_horas": None,
            "duracao_lifetime_dias": None,
            "atividade_mais_recente_id": None,
            "situacao_computed_at": now,
        },
        "unidades": [],
        "orgaos": [],
    }


def _days_since(iso: str | None, now_dt: datetime) -> int | None:
    dt = _parse_dt(iso)
    if dt is None:
        return None
    if dt.tzinfo is None and now_dt.tzinfo is not None:
        # Naive timestamps from SEI are local-time; treat them as such by
        # stripping tz from `now` for comparison.
        now_dt = now_dt.replace(tzinfo=None)
    elif dt.tzinfo is not None and now_dt.tzinfo is None:
        dt = dt.replace(tzinfo=None)
    delta = now_dt - dt
    return max(int(delta.total_seconds() // 86400), 0)


def _walk_units(
    sorted_activities: list[dict[str, Any]],
    protocolo: str,
) -> list[dict[str, Any]]:
    """Per-unit cycle detection. Returns one record per unit touched."""
    # state per unidade: {ciclos:[...], open_cycle:{...}|None,
    #                     last_significant:{...}|None,
    #                     all_atividades_data:[...] (for first/last)}
    state: dict[str, dict[str, Any]] = {}

    for atv in sorted_activities:
        unit = atv.get("unidade")
        if not unit:
            continue
        tipo = atv.get("tipo_acao")
        data_hora = atv.get("data_hora")
        sid = atv.get("source_id")

        st = state.setdefault(unit, {
            "ciclos": [],
            "open_cycle": None,
            "last_significant": None,
            "first_seen": data_hora,
            "last_seen": data_hora,
        })
        st["last_seen"] = data_hora

        if tipo in ACTIVATION_TYPES:
            # Edge case: an ACTIVATION arrives while a previous cycle is
            # still open (no intervening CONCLUSION). Common when a unit
            # receives a processo it had already received (REABERTURA on
            # an already-active unit). We close the prior cycle implicitly
            # at its own last activity to avoid lossy double-counting.
            if st["open_cycle"] is not None:
                _close_cycle_implicit(st["open_cycle"], st.get("last_seen"))
                st["ciclos"].append(st["open_cycle"])
                st["open_cycle"] = None

            ordem = len(st["ciclos"]) + (1 if st["open_cycle"] else 0)
            st["open_cycle"] = {
                "id": f"{protocolo}|{unit}|{ordem}",
                "ordem": ordem,
                "entrada": data_hora,
                "saida": None,
                "duracao_horas": 0.0,
                "status": "em_aberto",
                "abertura_atividade_id": sid,
                "abertura_tipo_acao": tipo,
                "conclusao_atividade_id": None,
                "conclusao_tipo_acao": None,
            }
            st["last_significant"] = atv

        elif tipo in CONCLUSION_TYPES:
            if st["open_cycle"] is not None:
                cycle = st["open_cycle"]
                cycle["saida"] = data_hora
                cycle["duracao_horas"] = round(
                    _hours_between(cycle["entrada"], data_hora), 2
                )
                cycle["status"] = "concluida"
                cycle["conclusao_atividade_id"] = sid
                cycle["conclusao_tipo_acao"] = tipo
                st["ciclos"].append(cycle)
                st["open_cycle"] = None
            else:
                # Anomaly: CONCLUSION without prior ACTIVATION on this unit
                # (data inconsistency or out-of-window window). Record as a
                # zero-duration concluded cycle anchored at the conclusion.
                ordem = len(st["ciclos"])
                st["ciclos"].append({
                    "id": f"{protocolo}|{unit}|{ordem}",
                    "ordem": ordem,
                    "entrada": data_hora,
                    "saida": data_hora,
                    "duracao_horas": 0.0,
                    "status": "concluida",
                    "abertura_atividade_id": None,
                    "abertura_tipo_acao": None,
                    "conclusao_atividade_id": sid,
                    "conclusao_tipo_acao": tipo,
                })
            st["last_significant"] = atv

        elif tipo == "PROCESSO-RECEBIDO-UNIDADE":
            # RECEBIDO é tie-break only — não abre nem fecha. O REMETIDO
            # correspondente já abriu o ciclo (vem antes via priority sort
            # em sort_and_fix_activities). Atualiza last_significant pra
            # que o ponteiro reflita a última ação relevante na unidade.
            st["last_significant"] = atv

        # Other activity types: ignored for cycle bookkeeping.

    # Finalize: any open cycle remains "em_aberto"; compute its provisional
    # duration up to the unit's last_seen activity.
    for unit, st in state.items():
        if st["open_cycle"] is not None:
            cycle = st["open_cycle"]
            cycle["duracao_horas"] = round(
                _hours_between(cycle["entrada"], st["last_seen"]), 2
            )
            st["ciclos"].append(cycle)
            st["open_cycle"] = None

    # ── Build per-unit records ──
    records: list[dict[str, Any]] = []
    for unit, st in state.items():
        ciclos = st["ciclos"]
        last = st["last_significant"]
        em_aberto = bool(ciclos) and ciclos[-1]["status"] == "em_aberto"

        # Aggregate durations
        duracao_acumulada = sum(
            c["duracao_horas"] for c in ciclos if c["status"] == "concluida"
        )
        primeira_entrada = ciclos[0]["entrada"] if ciclos else st["first_seen"]
        ultima_saida = (
            ciclos[-1]["saida"] if ciclos and ciclos[-1]["status"] == "concluida" else None
        )
        duracao_lifetime = _hours_between(primeira_entrada, st["last_seen"])

        records.append({
            "unidade": unit,
            "situacao": "em_aberto" if em_aberto else "concluida",
            "duracao_acumulada_horas": round(duracao_acumulada, 2),
            "duracao_lifetime_horas": round(duracao_lifetime, 2),
            "visitas": len(ciclos),
            "primeira_entrada": primeira_entrada,
            "ultima_saida": ultima_saida,
            "ultima_atividade_id": (last or {}).get("source_id"),
            "ultima_atividade_data_hora": (last or {}).get("data_hora"),
            "ultima_atividade_tipo_acao": (last or {}).get("tipo_acao"),
            "dias_sem_atividade": None,  # filled by caller with `now`
            "ciclos": ciclos,
        })

    return records


def _close_cycle_implicit(cycle: dict[str, Any], at_data_hora: str | None) -> None:
    """Mark a cycle as implicitly closed (no CONCLUSION found) at given time."""
    cycle["saida"] = at_data_hora
    cycle["duracao_horas"] = round(
        _hours_between(cycle["entrada"], at_data_hora), 2
    )
    # status stays "em_aberto" since we don't have evidence it was concluded;
    # the implicit close is purely structural to allow a new cycle to open.
    # Caller can treat as anomaly via cycle.implicit_close=True.
    cycle["implicit_close"] = True


def _aggregate_orgaos(
    unit_records: list[dict[str, Any]],
    fallback_ultima_atividade: str | None,
) -> list[dict[str, Any]]:
    """Aggregate per-orgao stats from per-unit records."""
    by_orgao: dict[str, dict[str, Any]] = {}
    for u in unit_records:
        orgao = extract_orgao(u["unidade"])
        agg = by_orgao.setdefault(orgao, {
            "orgao": orgao,
            "duracao_acumulada_horas": 0.0,
            "duracao_lifetime_horas": 0.0,
            "visitas": 0,
            "primeira_entrada": u["primeira_entrada"],
            "ultima_saida": u["ultima_saida"],
            "ultima_atividade_data_hora": u["ultima_atividade_data_hora"],
            "unidades_abertas": [],
            "unidades_total": [],
        })
        agg["duracao_acumulada_horas"] += u["duracao_acumulada_horas"]
        agg["duracao_lifetime_horas"] = max(agg["duracao_lifetime_horas"], u["duracao_lifetime_horas"])
        agg["visitas"] += u["visitas"]
        agg["unidades_total"].append(u["unidade"])
        if u["situacao"] == "em_aberto":
            agg["unidades_abertas"].append(u["unidade"])

        # Track temporal extremes
        if u["primeira_entrada"] and (
            not agg["primeira_entrada"] or u["primeira_entrada"] < agg["primeira_entrada"]
        ):
            agg["primeira_entrada"] = u["primeira_entrada"]
        # ultima_saida: max of all concluded; null if any unit still open
        if u["ultima_saida"] and (
            not agg["ultima_saida"] or u["ultima_saida"] > agg["ultima_saida"]
        ):
            agg["ultima_saida"] = u["ultima_saida"]
        if u["ultima_atividade_data_hora"] and (
            not agg["ultima_atividade_data_hora"]
            or u["ultima_atividade_data_hora"] > agg["ultima_atividade_data_hora"]
        ):
            agg["ultima_atividade_data_hora"] = u["ultima_atividade_data_hora"]

    return [
        {
            "orgao": agg["orgao"],
            "situacao": "em_aberto" if agg["unidades_abertas"] else "concluida",
            "unidades_abertas_count": len(agg["unidades_abertas"]),
            "unidades_abertas": sorted(agg["unidades_abertas"]),
            "duracao_acumulada_horas": round(agg["duracao_acumulada_horas"], 2),
            "duracao_lifetime_horas": round(agg["duracao_lifetime_horas"], 2),
            "visitas": agg["visitas"],
            "primeira_entrada": agg["primeira_entrada"],
            "ultima_saida": agg["ultima_saida"] if not agg["unidades_abertas"] else None,
            "ultima_atividade_data_hora": agg["ultima_atividade_data_hora"],
            "dias_sem_atividade": None,  # filled by caller with `now`
        }
        for agg in by_orgao.values()
    ]
