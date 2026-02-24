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

from .equipe import (
    EquipeCreate,
    EquipeUpdate,
    MembroAdd,
    MembroResponse,
    EquipeResponse,
    EquipeDetalheResponse,
)

from .tag import (
    TagCreate,
    TagUpdate,
    TagResponse,
    ProcessoSalvoCreate,
    ProcessoSalvoResponse,
    TagComProcessosResponse,
)

from .compartilhamento import (
    CompartilhamentoCreate,
    CompartilhamentoResponse,
    CompartilhadoComMigoItem,
)

from .observacao import (
    ObservacaoCreate,
    ObservacaoResponse,
)

from .team_tag import (
    TeamTagCreate,
    TeamTagUpdate,
    TeamTagResponse,
    ProcessoTeamTagCreate,
    ProcessoTeamTagResponse,
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
    "EquipeCreate",
    "EquipeUpdate",
    "MembroAdd",
    "MembroResponse",
    "EquipeResponse",
    "EquipeDetalheResponse",
    "TagCreate",
    "TagUpdate",
    "TagResponse",
    "ProcessoSalvoCreate",
    "ProcessoSalvoResponse",
    "TagComProcessosResponse",
    "CompartilhamentoCreate",
    "CompartilhamentoResponse",
    "CompartilhadoComMigoItem",
    "ObservacaoCreate",
    "ObservacaoResponse",
    "TeamTagCreate",
    "TeamTagUpdate",
    "TeamTagResponse",
    "ProcessoTeamTagCreate",
    "ProcessoTeamTagResponse",
]
