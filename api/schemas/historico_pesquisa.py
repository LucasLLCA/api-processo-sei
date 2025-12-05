"""
Schemas Pydantic para histórico de pesquisas
"""
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional
from uuid import UUID


class HistoricoPesquisaBase(BaseModel):
    """Schema base para histórico de pesquisas"""
    numero_processo: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Número do processo sem formatação",
        examples=["1234567890202499"]
    )
    numero_processo_formatado: Optional[str] = Field(
        None,
        max_length=50,
        description="Número do processo formatado",
        examples=["12345.678901/2024-99"]
    )
    usuario: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Identificação do usuário",
        examples=["joao.silva@email.com"]
    )
    caixa_contexto: Optional[str] = Field(
        None,
        description="Contexto adicional da pesquisa",
        examples=["Consulta via painel administrativo"]
    )


class HistoricoPesquisaCreate(HistoricoPesquisaBase):
    """Schema para criação de histórico de pesquisa"""
    pass


class HistoricoPesquisaUpdate(BaseModel):
    """Schema para atualização de histórico de pesquisa"""
    caixa_contexto: Optional[str] = Field(
        None,
        description="Novo contexto da pesquisa"
    )


class HistoricoPesquisaResponse(HistoricoPesquisaBase):
    """Schema de resposta com dados completos"""
    id: UUID = Field(..., description="ID único da pesquisa")
    criado_em: datetime = Field(..., description="Data de criação")
    atualizado_em: datetime = Field(..., description="Data de atualização")
    deletado_em: Optional[datetime] = Field(None, description="Data de exclusão (se deletado)")

    model_config = ConfigDict(from_attributes=True)


class HistoricoPesquisaList(BaseModel):
    """Schema para lista de pesquisas com paginação"""
    usuario: str = Field(..., description="Usuário consultado")
    total: int = Field(..., description="Total de pesquisas", ge=0)
    limit: int = Field(..., description="Limite por página", ge=1, le=100)
    offset: int = Field(..., description="Deslocamento da paginação", ge=0)
    pesquisas: list[HistoricoPesquisaResponse] = Field(
        ...,
        description="Lista de pesquisas"
    )


class HistoricoPesquisaSimple(BaseModel):
    """Schema simplificado para pesquisa"""
    id: UUID
    numero_processo: str
    numero_processo_formatado: Optional[str]
    caixa_contexto: Optional[str]
    criado_em: datetime

    model_config = ConfigDict(from_attributes=True)


class HistoricoPesquisaVerificacao(BaseModel):
    """Schema para verificar se processo já foi pesquisado"""
    numero_processo: str
    numero_processo_formatado: Optional[str]
    ja_pesquisado: bool
    total_pesquisas: int = Field(ge=0)
    ultima_pesquisa: Optional[dict] = None
    primeira_pesquisa: Optional[dict] = None


class HistoricoPesquisaStats(BaseModel):
    """Schema para estatísticas de processos mais pesquisados"""
    numero_processo: str
    numero_processo_formatado: Optional[str]
    total_pesquisas: int = Field(ge=0)
    total_usuarios: int = Field(ge=0)
    ultima_pesquisa: Optional[datetime] = None


class HistoricoPesquisaDeleteResponse(BaseModel):
    """Schema de resposta para exclusão"""
    message: str
    usuario: Optional[str] = None
    registros_apagados: Optional[int] = None
    deletado_em: Optional[datetime] = None
    id: Optional[UUID] = None


class HistoricoPesquisaRestoreResponse(BaseModel):
    """Schema de resposta para restauração"""
    message: str
    usuario: str
    registros_restaurados: int = Field(ge=0)
