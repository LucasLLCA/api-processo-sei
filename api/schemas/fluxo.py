"""
Schemas Pydantic para Fluxos de Processos
"""
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional, Literal
from uuid import UUID


# ── Request DTOs ──────────────────────────────────────────────


class FluxoCreate(BaseModel):
    nome: str = Field(..., min_length=1, max_length=200, description="Nome do fluxo")
    descricao: Optional[str] = Field(None, description="Descricao do fluxo")
    equipe_id: Optional[UUID] = Field(None, description="Equipe dona (NULL = pessoal)")
    orgao: Optional[str] = Field(None, max_length=50, description="Orgao para fluxo organizacional")


class FluxoUpdate(BaseModel):
    nome: Optional[str] = Field(None, min_length=1, max_length=200)
    descricao: Optional[str] = None
    status: Optional[Literal["rascunho", "publicado", "arquivado"]] = None


class FluxoNodeData(BaseModel):
    node_id: str = Field(..., max_length=100, description="ID do React Flow")
    tipo: Literal["sei_task", "etapa", "decisao", "inicio", "fim", "fork", "join"]
    nome: str = Field(..., min_length=1, max_length=300)
    descricao: Optional[str] = None
    sei_task_key: Optional[str] = Field(None, max_length=50)
    responsavel: Optional[str] = Field(None, max_length=200)
    duracao_estimada_horas: Optional[float] = None
    prioridade: Optional[Literal["baixa", "media", "alta", "critica"]] = None
    documentos_necessarios: Optional[list] = None
    checklist: Optional[list] = None
    regras_prazo: Optional[dict] = None
    metadata_extra: Optional[dict] = None
    posicao_x: float = Field(..., description="Posicao X no canvas")
    posicao_y: float = Field(..., description="Posicao Y no canvas")
    largura: Optional[float] = None
    altura: Optional[float] = None


class FluxoEdgeData(BaseModel):
    edge_id: str = Field(..., max_length=100, description="ID do React Flow")
    source_node_id: str = Field(..., max_length=100)
    target_node_id: str = Field(..., max_length=100)
    tipo: Literal["padrao", "condicional", "loop"] = "padrao"
    label: Optional[str] = Field(None, max_length=200)
    condicao: Optional[dict] = None
    ordem: Optional[int] = None
    animated: bool = False


class FluxoSaveCanvas(BaseModel):
    nodes: list[FluxoNodeData] = Field(..., description="Todos os nodes do canvas")
    edges: list[FluxoEdgeData] = Field(..., description="Todas as edges do canvas")
    viewport: Optional[dict] = Field(None, description="Viewport {x, y, zoom}")
    versao: int = Field(..., description="Versao atual para controle de concorrencia")


class FluxoProcessoCreate(BaseModel):
    numero_processo: str = Field(..., min_length=1, max_length=50)
    numero_processo_formatado: Optional[str] = Field(None, max_length=50)
    node_atual_id: Optional[str] = Field(None, max_length=100, description="Node inicial")
    notas: Optional[str] = None


class FluxoProcessoUpdate(BaseModel):
    node_atual_id: Optional[str] = Field(None, max_length=100)
    status: Optional[Literal["em_andamento", "concluido", "pausado", "cancelado"]] = None
    notas: Optional[str] = None


# ── Response DTOs ─────────────────────────────────────────────


class FluxoNodeResponse(BaseModel):
    id: UUID
    node_id: str
    tipo: str
    nome: str
    descricao: Optional[str] = None
    sei_task_key: Optional[str] = None
    responsavel: Optional[str] = None
    duracao_estimada_horas: Optional[float] = None
    prioridade: Optional[str] = None
    documentos_necessarios: Optional[list] = None
    checklist: Optional[list] = None
    regras_prazo: Optional[dict] = None
    metadata_extra: Optional[dict] = None
    posicao_x: float
    posicao_y: float
    largura: Optional[float] = None
    altura: Optional[float] = None

    model_config = ConfigDict(from_attributes=True)


class FluxoEdgeResponse(BaseModel):
    id: UUID
    edge_id: str
    source_node_id: str
    target_node_id: str
    tipo: str
    label: Optional[str] = None
    condicao: Optional[dict] = None
    ordem: Optional[int] = None
    animated: bool

    model_config = ConfigDict(from_attributes=True)


class FluxoResponse(BaseModel):
    id: UUID
    nome: str
    descricao: Optional[str] = None
    usuario: str
    equipe_id: Optional[UUID] = None
    orgao: Optional[str] = None
    versao: int
    status: str
    viewport: Optional[dict] = None
    node_count: int = 0
    edge_count: int = 0
    processo_count: int = 0
    criado_em: datetime
    atualizado_em: datetime

    model_config = ConfigDict(from_attributes=True)


class FluxoDetalheResponse(BaseModel):
    id: UUID
    nome: str
    descricao: Optional[str] = None
    usuario: str
    equipe_id: Optional[UUID] = None
    orgao: Optional[str] = None
    versao: int
    status: str
    viewport: Optional[dict] = None
    nodes: list[FluxoNodeResponse] = []
    edges: list[FluxoEdgeResponse] = []
    criado_em: datetime
    atualizado_em: datetime

    model_config = ConfigDict(from_attributes=True)


class FluxoProcessoResponse(BaseModel):
    id: UUID
    fluxo_id: UUID
    numero_processo: str
    numero_processo_formatado: Optional[str] = None
    node_atual_id: Optional[str] = None
    status: str
    iniciado_em: Optional[datetime] = None
    concluido_em: Optional[datetime] = None
    atribuido_por: str
    notas: Optional[str] = None
    historico: Optional[list] = None
    criado_em: datetime
    atualizado_em: datetime

    model_config = ConfigDict(from_attributes=True)
