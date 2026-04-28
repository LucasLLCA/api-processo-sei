"""SEI activity classifier.

Maps `sei_atividades.descricao_replace` text to task type codes and groups,
extracts metadata (source unidade, document references, bloco IDs), and
exposes a `transform_row` helper that turns a raw PostgreSQL row into the
flat dict shape consumed by the Phase B writer.

Groups mirror studio/src/lib/task-groups.ts exactly.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any


# ---------------------------------------------------------------------------
# Regex patterns: descricao_replace text → task type code
# Order matters — first match wins.
# Patterns handle both noun forms ("Disponibilização do bloco") and
# verb forms ("Bloco @BLOCO@ disponibilizado") used by SEI.
# ---------------------------------------------------------------------------
DESCRIPTION_PATTERNS: list[tuple[re.Pattern, str]] = [
    # ── Abertura ──
    (re.compile(r"^Processo\b.*\bgerado", re.IGNORECASE),         "GERACAO-PROCEDIMENTO"),
    (re.compile(r"^Processo atribuído", re.IGNORECASE),            "PROCESSO-ATRIBUIDO"),
    (re.compile(r"^Removida atribuição", re.IGNORECASE),           "PROCESSO-DESATRIBUIDO"),

    # ── Tramitação ──
    (re.compile(r"^Processo remetido", re.IGNORECASE),             "PROCESSO-REMETIDO-UNIDADE"),
    (re.compile(r"^Processo recebido", re.IGNORECASE),             "PROCESSO-RECEBIDO-UNIDADE"),
    (re.compile(r"^Conclusão do processo|^Processo concluído", re.IGNORECASE), "CONCLUSAO-PROCESSO-UNIDADE"),
    (re.compile(r"^Conclusão automática", re.IGNORECASE),          "CONCLUSAO-AUTOMATICA-UNIDADE"),
    (re.compile(r"^Reabertura do processo", re.IGNORECASE),        "REABERTURA-PROCESSO-UNIDADE"),
    (re.compile(r"^Sobrest(?:ado|amento|ando)", re.IGNORECASE),    "SOBRESTAMENTO"),
    (re.compile(r"^Remoção de sobrestamento", re.IGNORECASE),      "REMOCAO-SOBRESTAMENTO"),

    # ── Assinatura e Validação ──
    (re.compile(r"^Assinado Documento", re.IGNORECASE),            "ASSINATURA-DOCUMENTO"),
    (re.compile(r"^Autenticado Documento", re.IGNORECASE),         "AUTENTICACAO-DOCUMENTO"),
    (re.compile(r"^Cancelamento de assinatura", re.IGNORECASE),    "CANCELAMENTO-ASSINATURA"),
    (re.compile(r"^Cancelada liberação de assinatura", re.IGNORECASE), "CANCELAMENTO-LIBERACAO-ASSINATURA"),
    (re.compile(r"^Liber(?:ação|ada)\b.*assinatura externa", re.IGNORECASE), "LIBERACAO-ASSINATURA-EXTERNA"),
    (re.compile(r"^Ciência no documento", re.IGNORECASE),          "DOCUMENTO-CIENCIA"),
    (re.compile(r"^Ciência no processo", re.IGNORECASE),           "PROCESSO-CIENCIA"),

    # ── Criação de Documentos ──
    (re.compile(r"^Gerado documento|^Documento.*gerado", re.IGNORECASE), "GERACAO-DOCUMENTO"),
    (re.compile(r"^Arquivo.*anexado", re.IGNORECASE),              "ARQUIVO-ANEXADO"),
    (re.compile(r"^Processo\b.*\banexado|^Anexado ao processo", re.IGNORECASE), "PROCESSO-ANEXADO"),
    (re.compile(r"^Documento\b.*\bmovido", re.IGNORECASE),         "DOCUMENTO-MOVIDO"),
    (re.compile(r"^Registro de documento externo", re.IGNORECASE), "RECEBIMENTO-DOCUMENTO"),

    # ── Blocos (specific cancel patterns BEFORE generic cancel) ──
    (re.compile(r"^Documento.*inserido no bloco", re.IGNORECASE),  "DOCUMENTO-INCLUIDO-EM-BLOCO"),
    (re.compile(r"^Documento.*retirado do bloco", re.IGNORECASE),  "DOCUMENTO-RETIRADO-DO-BLOCO"),
    (re.compile(r"^Processo inserido no bloco", re.IGNORECASE),    "PROCESSO-INCLUIDO-EM-BLOCO"),
    (re.compile(r"^Processo retirado do bloco", re.IGNORECASE),    "PROCESSO-RETIRADO-DO-BLOCO"),
    (re.compile(r"^Disponibilização do bloco|^Bloco\b.*\bdisponibilizado", re.IGNORECASE), "BLOCO-DISPONIBILIZACAO"),
    (re.compile(r"^Retorno do bloco|^Bloco\b.*\bretornado", re.IGNORECASE), "BLOCO-RETORNO"),
    (re.compile(r"^Conclusão do bloco", re.IGNORECASE),            "BLOCO-CONCLUSAO"),
    (re.compile(r"^Reabertura do bloco", re.IGNORECASE),           "BLOCO-REABERTURA"),
    (re.compile(r"Cancel.*disponibilização.*bloco|Cancel.*disponibilização do bloco", re.IGNORECASE), "BLOCO-CANCELAMENTO-DISPONIBILIZACAO"),

    # ── Edição e Manutenção ──
    (re.compile(r"^Anexo\b.*removido do documento", re.IGNORECASE),"REMOCAO-ANEXO"),
    (re.compile(r"^Cancel(?:amento|ado)\b.*documento", re.IGNORECASE), "CANCELAMENTO-DOCUMENTO"),
    (re.compile(r"^Exclusão do documento", re.IGNORECASE),         "EXCLUSAO-DOCUMENTO"),
    (re.compile(r"^Alter(?:ação|ado)\b.*nível de acesso.*documento", re.IGNORECASE), "ALTERACAO-NIVEL-ACESSO-DOCUMENTO"),
    (re.compile(r"^Alter(?:ação|ado)\b.*nível de acesso.*(?:processo|geral)", re.IGNORECASE), "ALTERACAO-NIVEL-ACESSO-PROCESSO"),
    (re.compile(r"^Alter(?:ação|ado)\b.*tipo d[oe] processo", re.IGNORECASE), "ALTERACAO-TIPO-PROCESSO"),
    (re.compile(r"^Alter(?:ação|ada|ado)\b.*hipótese legal", re.IGNORECASE), "ALTERACAO-HIPOTESE-LEGAL"),
    (re.compile(r"^Alter(?:ação|ado)\b.*tipo de conferência", re.IGNORECASE), "ALTERACAO-TIPO-CONFERENCIA"),
    (re.compile(r"^Alter(?:ação|ada)\b.*ordem", re.IGNORECASE),    "PROCESSO-ALTERACAO-ORDEM-ARVORE"),
    (re.compile(r"^Atualização de andamento", re.IGNORECASE),      "ATUALIZACAO-ANDAMENTO"),

    # ── Comunicação ──
    (re.compile(r"^Disponibilizado acesso externo", re.IGNORECASE), "ACESSO-EXTERNO-SISTEMA"),
    (re.compile(r"Cancel.*disponibilização.*acesso externo", re.IGNORECASE), "CANCELAMENTO-ACESSO-EXTERNO"),
    (re.compile(r"^Enviado e-mail|^Envio de e-mail|^Envio de correspond", re.IGNORECASE), "ENVIO-EMAIL"),
    (re.compile(r"Usu[aá]rio Externo.*Peticionamento", re.IGNORECASE), "PETICIONAMENTO-EXTERNO"),
    (re.compile(r"^Credencial concedida", re.IGNORECASE),          "CREDENCIAL-CONCEDIDA"),

    # ── Catch-all for SEI template placeholders ──
    (re.compile(r"^@DESCRICAO@$"),                                 "DESCRICAO-GENERICA"),
]

# ---------------------------------------------------------------------------
# Task type → group mapping (mirrors studio/src/lib/task-groups.ts)
# ---------------------------------------------------------------------------
TASK_GROUPS: dict[str, dict] = {
    "criacao":     {"label": "Criação de Documentos",   "horas": 0.5, "tasks": ["GERACAO-DOCUMENTO", "ARQUIVO-ANEXADO", "RECEBIMENTO-DOCUMENTO"]},
    "assinatura":  {"label": "Assinatura e Validação",  "horas": 0,   "tasks": ["ASSINATURA-DOCUMENTO", "AUTENTICACAO-DOCUMENTO", "CANCELAMENTO-ASSINATURA", "CANCELAMENTO-LIBERACAO-ASSINATURA", "LIBERACAO-ASSINATURA-EXTERNA", "DOCUMENTO-CIENCIA", "PROCESSO-CIENCIA"]},
    "tramitacao":  {"label": "Tramitação",              "horas": 1,   "tasks": ["PROCESSO-REMETIDO-UNIDADE", "PROCESSO-RECEBIDO-UNIDADE", "CONCLUSAO-PROCESSO-UNIDADE", "REABERTURA-PROCESSO-UNIDADE", "SOBRESTAMENTO", "REMOCAO-SOBRESTAMENTO"]},
    "blocos":      {"label": "Gestão de Blocos",        "horas": 0,   "tasks": ["DOCUMENTO-INCLUIDO-EM-BLOCO", "DOCUMENTO-RETIRADO-DO-BLOCO", "PROCESSO-INCLUIDO-EM-BLOCO", "PROCESSO-RETIRADO-DO-BLOCO", "BLOCO-DISPONIBILIZACAO", "BLOCO-RETORNO", "BLOCO-CONCLUSAO", "BLOCO-REABERTURA", "BLOCO-CANCELAMENTO-DISPONIBILIZACAO"]},
    "edicao":      {"label": "Edição e Manutenção",     "horas": 0,   "tasks": ["CANCELAMENTO-DOCUMENTO", "EXCLUSAO-DOCUMENTO", "REMOCAO-ANEXO", "ALTERACAO-NIVEL-ACESSO-DOCUMENTO", "ALTERACAO-NIVEL-ACESSO-PROCESSO", "ALTERACAO-TIPO-PROCESSO", "ALTERACAO-HIPOTESE-LEGAL", "ALTERACAO-TIPO-CONFERENCIA", "PROCESSO-ALTERACAO-ORDEM-ARVORE", "ATUALIZACAO-ANDAMENTO", "PROCESSO-ANEXADO", "DOCUMENTO-MOVIDO"]},
    "abertura":    {"label": "Abertura e Atribuição",   "horas": 0,   "tasks": ["GERACAO-PROCEDIMENTO", "PROCESSO-ATRIBUIDO", "PROCESSO-DESATRIBUIDO"]},
    "comunicacao": {"label": "Acesso e Comunicação",    "horas": 0,   "tasks": ["ACESSO-EXTERNO-SISTEMA", "CANCELAMENTO-ACESSO-EXTERNO", "ENVIO-EMAIL", "PETICIONAMENTO-EXTERNO", "CREDENCIAL-CONCEDIDA"]},
    "automatico":  {"label": "Automático",              "horas": 0,   "tasks": ["CONCLUSAO-AUTOMATICA-UNIDADE"]},
    "outros":      {"label": "Outros",                  "horas": 0,   "tasks": ["DESCRICAO-GENERICA"]},
}

# Reverse map: task code → group key
_TASK_TO_GROUP: dict[str, str] = {}
for _group_key, _group_info in TASK_GROUPS.items():
    for _task in _group_info["tasks"]:
        _TASK_TO_GROUP[_task] = _group_key

# Regex to extract source unit from tramitação descriptions
_REMETIDO_RE = re.compile(r"remetido pela unidade\s+(\S+)", re.IGNORECASE)

# Regex to extract bloco or document reference IDs from descriptions
_BLOCO_REF_RE = re.compile(r"\bbloco\s+(\d+)", re.IGNORECASE)
_DOC_REF_RE = re.compile(r"\bdocumento\s+(?:\w+\s+)*?(\d{6,})", re.IGNORECASE)

# Regex to extract full document info: number + (Type SeriesID)
# Matches: "0020032608 (SEAD_OFICIO 13441)", "0020030371 (Ficha)", "016096560 (Aviso 445)"
_DOC_INFO_RE = re.compile(r"(\d{7,})\s*\(([^@)]+?)(?:\s+(\d+))?\)")


def classify_descricao(text: str | None) -> str:
    """Classify descricao_replace text into a SEI task type code."""
    if not text:
        return "OUTROS"
    for pattern, task_type in DESCRIPTION_PATTERNS:
        if pattern.search(text):
            return task_type
    return "OUTROS"


def get_grupo(tipo_acao: str) -> str:
    """Map a task type code to its group key."""
    return _TASK_TO_GROUP.get(tipo_acao, "outros")


def extract_source_unidade(text: str | None) -> str | None:
    """Extract the source unit from 'Processo remetido pela unidade X' descriptions."""
    if not text:
        return None
    m = _REMETIDO_RE.search(text)
    return m.group(1) if m else None


def extract_document_info(text: str | None) -> dict | None:
    """Extract document number, type, and series ID from a description.

    Matches patterns like:
      - "0020032608 (SEAD_OFICIO 13441)"  → {numero, tipo, serie_id}
      - "0020030371 (Ficha)"              → {numero, tipo, serie_id=None}
      - "016096560 (Aviso 445)"           → {numero, tipo, serie_id}

    Returns dict with keys {numero, tipo, serie_id} or None.
    """
    if not text:
        return None
    m = _DOC_INFO_RE.search(text)
    if not m:
        return None
    return {
        "numero": m.group(1),
        "tipo": m.group(2).strip(),
        "serie_id": m.group(3),
    }


def extract_reference_id(text: str | None) -> str | None:
    """Extract a bloco or document reference ID from a description.

    Returns 'bloco:NNN' or 'doc:NNN', or None if no reference found.
    Bloco takes priority since bloco-related actions are the main
    source of independent (non-flow) activities.
    """
    if not text:
        return None
    m = _BLOCO_REF_RE.search(text)
    if m:
        return f"bloco:{m.group(1)}"
    m = _DOC_REF_RE.search(text)
    if m:
        return f"doc:{m.group(1)}"
    return None


def get_all_tipo_acao_records() -> list[dict]:
    """Return all known task types with their group for seeding Neo4j."""
    records = []
    for group_key, group_info in TASK_GROUPS.items():
        for task in group_info["tasks"]:
            records.append({"chave": task, "grupo": group_key})
    records.append({"chave": "OUTROS", "grupo": "outros"})
    return records


def get_all_grupo_records() -> list[dict]:
    """Return all groups for seeding Neo4j."""
    return [
        {"chave": key, "label": info["label"], "horas": info["horas"]}
        for key, info in TASK_GROUPS.items()
    ]


# ---------------------------------------------------------------------------
# Row transformation
# ---------------------------------------------------------------------------
def transform_row(row: dict[str, Any], seq: int) -> dict[str, Any]:
    """Transform a raw sei_atividades row into a flat graph atividade dict.

    Reads `descricao_replace`, classifies it, extracts metadata, normalizes
    the timestamp, and packages the result for `load_atividades_batch`.
    """
    descricao = row.get("descricao_replace") or ""
    tipo_acao = classify_descricao(descricao)
    grupo = get_grupo(tipo_acao)
    is_creation = tipo_acao == "GERACAO-PROCEDIMENTO"
    source_unidade = (
        extract_source_unidade(descricao)
        if tipo_acao == "PROCESSO-REMETIDO-UNIDADE"
        else None
    )
    ref_id = extract_reference_id(descricao)
    doc_info = extract_document_info(descricao)

    data_hora_str: str | None = None
    if row.get("data_hora"):
        dt = row["data_hora"]
        data_hora_str = (
            dt.strftime("%Y-%m-%dT%H:%M:%S") if isinstance(dt, datetime) else str(dt)
        )

    return {
        "source_id": row["id"],
        "protocolo_formatado": row["protocolo_formatado"],
        "data_hora": data_hora_str,
        "unidade": row.get("unidade") or "DESCONHECIDA",
        "usuario": row.get("usuario") or "DESCONHECIDO",
        "tipo_procedimento": row.get("tipo_procedimento"),
        "descricao": descricao,
        "tipo_acao": tipo_acao,
        "grupo": grupo,
        "is_creation": is_creation,
        "source_unidade": source_unidade,
        "ref_id": ref_id,
        "doc_info": doc_info,
        "seq": seq,
    }
