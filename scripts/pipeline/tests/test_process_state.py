"""Tests for ``pipeline.process_state.compute_processo_state``.

Covers the cycle-detection algorithm under realistic SEI patterns:
- single-unit open / closed
- multi-unit linear flow with REMETIDO/RECEBIDO/CONCLUSAO
- reabertura (REABERTURA-PROCESSO-UNIDADE)
- parallel branches (multiple units active at once)
- anomalies (CONCLUSION without prior ACTIVATION; empty input)

REMETIDO does NOT close the source unit's cycle — confirmed with the user.
"""

from __future__ import annotations

import pytest

from pipeline.process_state import (
    ACTIVATION_TYPES,
    CONCLUSION_TYPES,
    compute_processo_state,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------
def _atv(source_id, data_hora, unidade, tipo_acao, source_unidade=None, ref_id=None):
    return {
        "source_id": source_id,
        "data_hora": data_hora,
        "unidade": unidade,
        "tipo_acao": tipo_acao,
        "source_unidade": source_unidade,
        "ref_id": ref_id,
    }


PF = "00001.000001/2025-01"
NOW = "2025-06-01T00:00:00"


# ---------------------------------------------------------------------------
# Simple cases
# ---------------------------------------------------------------------------
def test_empty_returns_well_formed_state():
    state = compute_processo_state([], protocolo_formatado=PF, now_iso=NOW)
    assert state["processo"]["situacao"] == "em_andamento"
    assert state["processo"]["unidades_em_aberto_count"] == 0
    assert state["unidades"] == []
    assert state["orgaos"] == []


def test_single_unit_opened_never_closed_is_em_aberto():
    activities = [
        _atv(1, "2025-01-01T10:00:00", "SEAD-PI/GAB", "GERACAO-PROCEDIMENTO"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)

    assert state["processo"]["situacao"] == "em_andamento"
    assert state["processo"]["unidades_em_aberto"] == ["SEAD-PI/GAB"]
    assert state["processo"]["data_conclusao_global"] is None
    assert state["processo"]["duracao_lifetime_horas"] is None

    assert len(state["unidades"]) == 1
    u = state["unidades"][0]
    assert u["unidade"] == "SEAD-PI/GAB"
    assert u["situacao"] == "em_aberto"
    assert len(u["ciclos"]) == 1
    assert u["ciclos"][0]["status"] == "em_aberto"
    assert u["ciclos"][0]["abertura_atividade_id"] == 1
    assert u["ciclos"][0]["conclusao_atividade_id"] is None


def test_single_unit_opened_then_closed_is_concluida():
    activities = [
        _atv(1, "2025-01-01T10:00:00", "SEAD-PI/GAB", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-02T10:00:00", "SEAD-PI/GAB", "CONCLUSAO-PROCESSO-UNIDADE"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)

    assert state["processo"]["situacao"] == "concluido"
    assert state["processo"]["unidades_em_aberto"] == []
    assert state["processo"]["data_conclusao_global"] == "2025-01-02T10:00:00"
    assert state["processo"]["duracao_lifetime_horas"] == 24.0
    assert state["processo"]["duracao_lifetime_dias"] == 1.0

    u = state["unidades"][0]
    assert u["situacao"] == "concluida"
    assert u["ciclos"][0]["status"] == "concluida"
    assert u["ciclos"][0]["duracao_horas"] == 24.0
    assert u["ciclos"][0]["conclusao_atividade_id"] == 2


# ---------------------------------------------------------------------------
# Multi-unit flow
# ---------------------------------------------------------------------------
def test_remetido_opens_new_cycle_on_destination():
    """Inbox-SEI semantics: PROCESSO-REMETIDO-UNIDADE opens a cycle on the
    destination unit (since it lands the processo in that unit's inbox).

    If the destination only got a REMETIDO (no follow-up CONCLUSAO), its
    cycle stays open. Two ACTIVATIONs in a row implicitly close the first.
    """
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-02T10:00:00", "A", "PROCESSO-REMETIDO-UNIDADE"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)
    a = state["unidades"][0]
    # Two ACTIVATIONs (GERACAO then REMETIDO) → first cycle implicitly closed
    # (implicit_close=True; status remains em_aberto since there's no CONCLUSAO),
    # second cycle remains open.
    assert len(a["ciclos"]) == 2
    assert a["ciclos"][0].get("implicit_close") is True
    assert a["ciclos"][1]["status"] == "em_aberto"
    assert a["ciclos"][1]["abertura_tipo_acao"] == "PROCESSO-REMETIDO-UNIDADE"
    assert a["situacao"] == "em_aberto"
    assert state["processo"]["situacao"] == "em_andamento"


def test_full_tramitacao_a_to_b_b_concludes():
    """A receives, sends to B, B receives, B concludes.

    SEI semantics: REMETIDO is logged on the destination unit. So we see
    PROCESSO-REMETIDO-UNIDADE rows whose ``unidade`` equals the destination.
    For this test we keep things simple — no auto-conclusao on A — so A stays
    open per the user-confirmed rule. Realistic SEI runs include
    CONCLUSAO-AUTOMATICA-UNIDADE on A; that is covered in the next test.
    """
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-02T10:00:00", "B", "PROCESSO-REMETIDO-UNIDADE", source_unidade="A"),
        _atv(3, "2025-01-02T10:00:30", "B", "PROCESSO-RECEBIDO-UNIDADE"),
        _atv(4, "2025-01-05T10:00:00", "B", "CONCLUSAO-PROCESSO-UNIDADE"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)

    units_by_name = {u["unidade"]: u for u in state["unidades"]}
    assert units_by_name["A"]["situacao"] == "em_aberto"  # No CONCLUSAO on A
    assert units_by_name["B"]["situacao"] == "concluida"
    assert state["processo"]["situacao"] == "em_andamento"  # A still open
    assert state["processo"]["unidades_em_aberto"] == ["A"]


def test_full_tramitacao_with_auto_conclusao_on_source():
    """A receives, A auto-concludes (typical SEI), sends to B, B concludes."""
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-02T10:00:00", "A", "CONCLUSAO-AUTOMATICA-UNIDADE"),
        _atv(3, "2025-01-02T10:00:01", "B", "PROCESSO-REMETIDO-UNIDADE", source_unidade="A"),
        _atv(4, "2025-01-02T10:00:02", "B", "PROCESSO-RECEBIDO-UNIDADE"),
        _atv(5, "2025-01-05T10:00:00", "B", "CONCLUSAO-PROCESSO-UNIDADE"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)
    by = {u["unidade"]: u for u in state["unidades"]}
    assert by["A"]["situacao"] == "concluida"
    assert by["B"]["situacao"] == "concluida"
    assert state["processo"]["situacao"] == "concluido"
    assert state["processo"]["data_conclusao_global"] == "2025-01-05T10:00:00"
    assert state["processo"]["duracao_lifetime_horas"] == 96.0  # 4 days


# ---------------------------------------------------------------------------
# Reabertura
# ---------------------------------------------------------------------------
def test_reabertura_creates_second_cycle_on_same_unit():
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-05T10:00:00", "A", "CONCLUSAO-PROCESSO-UNIDADE"),
        _atv(3, "2025-02-01T10:00:00", "A", "REABERTURA-PROCESSO-UNIDADE"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)
    a = state["unidades"][0]
    assert len(a["ciclos"]) == 2
    assert a["ciclos"][0]["status"] == "concluida"
    assert a["ciclos"][1]["status"] == "em_aberto"
    assert a["situacao"] == "em_aberto"
    assert state["processo"]["situacao"] == "em_andamento"


def test_reabertura_then_conclusion_closes_second_cycle():
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-05T10:00:00", "A", "CONCLUSAO-PROCESSO-UNIDADE"),
        _atv(3, "2025-02-01T10:00:00", "A", "REABERTURA-PROCESSO-UNIDADE"),
        _atv(4, "2025-02-10T10:00:00", "A", "CONCLUSAO-PROCESSO-UNIDADE"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)
    a = state["unidades"][0]
    assert len(a["ciclos"]) == 2
    assert all(c["status"] == "concluida" for c in a["ciclos"])
    assert a["situacao"] == "concluida"
    assert state["processo"]["situacao"] == "concluido"


# ---------------------------------------------------------------------------
# Parallelism
# ---------------------------------------------------------------------------
def test_parallel_units_independent_state():
    """A and B both active at the same time; A concludes; B stays open.

    Inbox-SEI: B opens via REMETIDO; subsequent RECEBIDO is tie-break, not
    a separate ACTIVATION.
    """
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-02T10:00:00", "B", "PROCESSO-REMETIDO-UNIDADE", source_unidade="A"),
        _atv(3, "2025-01-02T10:00:30", "B", "PROCESSO-RECEBIDO-UNIDADE"),
        _atv(4, "2025-01-03T10:00:00", "A", "CONCLUSAO-PROCESSO-UNIDADE"),
        # B is still open
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)
    by = {u["unidade"]: u for u in state["unidades"]}
    assert by["A"]["situacao"] == "concluida"
    assert by["B"]["situacao"] == "em_aberto"
    assert state["processo"]["situacao"] == "em_andamento"
    assert state["processo"]["unidades_em_aberto"] == ["B"]


# ---------------------------------------------------------------------------
# Anomalies / data quality
# ---------------------------------------------------------------------------
def test_conclusion_without_prior_activation_creates_zero_duration_cycle():
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "CONCLUSAO-PROCESSO-UNIDADE"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)
    a = state["unidades"][0]
    assert len(a["ciclos"]) == 1
    assert a["ciclos"][0]["status"] == "concluida"
    assert a["ciclos"][0]["duracao_horas"] == 0.0
    assert a["ciclos"][0]["abertura_atividade_id"] is None
    assert a["ciclos"][0]["conclusao_atividade_id"] == 1


def test_double_activation_implicitly_closes_first_cycle():
    """Two consecutive ACTIVATIONs without a CONCLUSAO between them: the
    first cycle is implicitly closed at its last_seen so the second can open."""
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-05T10:00:00", "A", "REABERTURA-PROCESSO-UNIDADE"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)
    a = state["unidades"][0]
    assert len(a["ciclos"]) == 2
    # First cycle was implicitly closed
    assert a["ciclos"][0].get("implicit_close") is True
    # Second cycle remains open
    assert a["ciclos"][1]["status"] == "em_aberto"


# ---------------------------------------------------------------------------
# Aggregations
# ---------------------------------------------------------------------------
def test_orgao_aggregates_multiple_subunits():
    """SEAD-PI has two sub-units; one open, one concluded → orgao em_aberto."""
    activities = [
        _atv(1, "2025-01-01T10:00:00", "SEAD-PI/GAB", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-02T10:00:00", "SEAD-PI/GAB", "CONCLUSAO-PROCESSO-UNIDADE"),
        _atv(3, "2025-01-03T10:00:00", "SEAD-PI/SGACG", "PROCESSO-REMETIDO-UNIDADE",
             source_unidade="SEAD-PI/GAB"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)
    assert len(state["orgaos"]) == 1
    o = state["orgaos"][0]
    assert o["orgao"] == "SEAD-PI"
    assert o["situacao"] == "em_aberto"
    assert o["unidades_abertas_count"] == 1
    assert "SEAD-PI/SGACG" in o["unidades_abertas"]


def test_processo_atividade_mais_recente_id():
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "GERACAO-PROCEDIMENTO"),
        _atv(2, "2025-01-02T10:00:00", "A", "ATUALIZACAO-ANDAMENTO"),
        _atv(3, "2025-01-03T10:00:00", "A", "CONCLUSAO-PROCESSO-UNIDADE"),
    ]
    state = compute_processo_state(activities, protocolo_formatado=PF, now_iso=NOW)
    assert state["processo"]["atividade_mais_recente_id"] == 3


def test_dias_sem_atividade_snapshot():
    activities = [
        _atv(1, "2025-01-01T10:00:00", "A", "GERACAO-PROCEDIMENTO"),
    ]
    # 30 days later
    state = compute_processo_state(
        activities, protocolo_formatado=PF, now_iso="2025-01-31T10:00:00",
    )
    assert state["unidades"][0]["dias_sem_atividade"] == 30


# ---------------------------------------------------------------------------
# Type set sanity
# ---------------------------------------------------------------------------
def test_activation_and_conclusion_sets_are_disjoint():
    assert ACTIVATION_TYPES & CONCLUSION_TYPES == set()


def test_remetido_in_activation_set():
    """Inbox-SEI: REMETIDO opens the destination unit (lands in inbox)."""
    assert "PROCESSO-REMETIDO-UNIDADE" in ACTIVATION_TYPES
    assert "PROCESSO-REMETIDO-UNIDADE" not in CONCLUSION_TYPES


def test_recebido_not_in_either_set():
    """RECEBIDO is tie-break only — does not open or close."""
    assert "PROCESSO-RECEBIDO-UNIDADE" not in ACTIVATION_TYPES
    assert "PROCESSO-RECEBIDO-UNIDADE" not in CONCLUSION_TYPES
