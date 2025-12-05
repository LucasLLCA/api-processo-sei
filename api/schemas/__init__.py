"""
Schemas Pydantic para validação e serialização
"""
from .historico_pesquisa import (
    HistoricoPesquisaCreate,
    HistoricoPesquisaUpdate,
    HistoricoPesquisaResponse,
    HistoricoPesquisaList,
    HistoricoPesquisaSimple,
    HistoricoPesquisaVerificacao,
    HistoricoPesquisaStats,
    HistoricoPesquisaDeleteResponse,
    HistoricoPesquisaRestoreResponse,
)

__all__ = [
    "HistoricoPesquisaCreate",
    "HistoricoPesquisaUpdate",
    "HistoricoPesquisaResponse",
    "HistoricoPesquisaList",
    "HistoricoPesquisaSimple",
    "HistoricoPesquisaVerificacao",
    "HistoricoPesquisaStats",
    "HistoricoPesquisaDeleteResponse",
    "HistoricoPesquisaRestoreResponse",
]
