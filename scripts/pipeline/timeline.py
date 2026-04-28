"""Pure-Python timeline DAG construction for SEI process flows.

Produces SEGUIDA_POR / SEGUIDO_INDEPENDENTEMENTE_POR edges using
unidade-context tracking instead of naive timestamp grouping. Activities
chain within the same unidade; transitions happen via REMETIDO/RECEBIDO
pairs. Parallel branches stay independent.

Mirrors studio/src/lib/process-flow-utils.ts for frontend/backend parity.
"""

from __future__ import annotations

from datetime import datetime


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

FLOW_ACTIVATION_TYPES = {
    "GERACAO-PROCEDIMENTO",
    "PROCESSO-RECEBIDO-UNIDADE",
    "REABERTURA-PROCESSO-UNIDADE",
}


def transfer_priority(tipo_acao: str) -> int:
    """Same-timestamp tiebreaker: conclusão(0) → remetido(1) → recebido(2) → others(1)."""
    if tipo_acao in CONCLUSION_TYPES:
        return 0
    if tipo_acao == "PROCESSO-REMETIDO-UNIDADE":
        return 1
    if tipo_acao == "PROCESSO-RECEBIDO-UNIDADE":
        return 2
    return 1


def _parse_dt(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return datetime.min


def sort_and_fix_activities(activities: list[dict]) -> list[dict]:
    """Sort with priority tiebreaker + 60s recebido/remetido fixup."""
    activities.sort(
        key=lambda a: (
            a.get("data_hora", ""),
            transfer_priority(a["tipo_acao"]),
            a["source_id"],
        )
    )

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
            if (cand_time - recebido_time).total_seconds() > 60:
                break
            if candidate["tipo_acao"] in (
                "PROCESSO-REMETIDO-UNIDADE",
                "CONCLUSAO-AUTOMATICA-UNIDADE",
                "CONCLUSAO-PROCESSO-UNIDADE",
            ):
                activities.insert(i, activities.pop(j))
            else:
                j += 1
        i += 1

    return activities


def compute_interval(from_atv: dict, to_atv: dict) -> tuple[float | None, float | None]:
    """Time delta in (hours, days), or (None, None) if either timestamp is missing."""
    from_dt = from_atv.get("data_hora", "")
    to_dt = to_atv.get("data_hora", "")
    if not from_dt or not to_dt:
        return None, None
    try:
        delta = datetime.fromisoformat(to_dt) - datetime.fromisoformat(from_dt)
        total = delta.total_seconds()
        return round(total / 3600, 2), round(total / 86400, 2)
    except (ValueError, TypeError):
        return None, None


def build_edges_for_processo(
    activities: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Build (flow_edges, independent_edges) for one processo's activities.

    Rules:
      1. Same unidade as previous: chain to last activity there.
      2. REMETIDO is logged at the destination unidade — connect from last
         activity at the SOURCE unidade (extracted from description).
      3. RECEBIDO connects from pending REMETIDO(s) targeting this unidade.
      4. Activities from units that never had GERACAO/RECEBIDO/REABERTURA
         are linked independently via shared bloco/document reference.
      5. Conclusão nodes do NOT produce outgoing SEGUIDA_POR edges.
      6. Cross-unit document references create independent edges.
    """
    if len(activities) < 2:
        return [], []

    activities = sort_and_fix_activities(activities)

    activated_units: set[str] = set()
    for atv in activities:
        if atv["tipo_acao"] in FLOW_ACTIVATION_TYPES:
            activated_units.add(atv["unidade"])

    first_by_ref: dict[str, dict] = {}
    for atv in activities:
        rid = atv.get("ref_id")
        if rid and rid not in first_by_ref:
            first_by_ref[rid] = atv

    flow_edges: list[dict] = []
    independent_edges: list[dict] = []
    last_at: dict[str, dict] = {}
    pending_remetidos: dict[str, list[dict]] = {}

    for atv in activities:
        u = atv["unidade"]
        tipo = atv["tipo_acao"]
        sid = atv["source_id"]

        rid = atv.get("ref_id")
        if rid and rid in first_by_ref:
            origin = first_by_ref[rid]
            if origin["source_id"] != sid and origin["unidade"] != u:
                independent_edges.append(
                    {"from_id": origin["source_id"], "to_id": sid, "ref_id": rid}
                )

        if u not in activated_units:
            continue

        if tipo == "PROCESSO-REMETIDO-UNIDADE":
            src = atv.get("source_unidade")
            if src and src in last_at:
                if last_at[src]["tipo_acao"] not in CONCLUSION_TYPES:
                    h, d = compute_interval(last_at[src], atv)
                    flow_edges.append({
                        "from_id": last_at[src]["source_id"],
                        "to_id": sid,
                        "mesma_unidade": False,
                        "intervalo_horas": h,
                        "intervalo_dias": d,
                    })
            elif u in last_at:
                if last_at[u]["tipo_acao"] not in CONCLUSION_TYPES:
                    h, d = compute_interval(last_at[u], atv)
                    flow_edges.append({
                        "from_id": last_at[u]["source_id"],
                        "to_id": sid,
                        "mesma_unidade": True,
                        "intervalo_horas": h,
                        "intervalo_dias": d,
                    })
            pending_remetidos.setdefault(u, []).append(atv)

        elif tipo == "PROCESSO-RECEBIDO-UNIDADE":
            if u in pending_remetidos and pending_remetidos[u]:
                for rem in pending_remetidos[u]:
                    h, d = compute_interval(rem, atv)
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
                    h, d = compute_interval(last_at[u], atv)
                    flow_edges.append({
                        "from_id": last_at[u]["source_id"],
                        "to_id": sid,
                        "mesma_unidade": True,
                        "intervalo_horas": h,
                        "intervalo_dias": d,
                    })

        else:
            if u in last_at and last_at[u]["tipo_acao"] not in CONCLUSION_TYPES:
                h, d = compute_interval(last_at[u], atv)
                flow_edges.append({
                    "from_id": last_at[u]["source_id"],
                    "to_id": sid,
                    "mesma_unidade": True,
                    "intervalo_horas": h,
                    "intervalo_dias": d,
                })

        last_at[u] = atv

    return flow_edges, independent_edges
