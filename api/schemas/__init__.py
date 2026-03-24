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
    MoverProcessoKanban,
    SalvarProcessoKanban,
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
    ObservacaoUpdate,
    ObservacaoResponse,
    ObservacaoMencaoResponse,
)

from .team_tag import (
    TeamTagCreate,
    TeamTagUpdate,
    TeamTagResponse,
    ProcessoTeamTagCreate,
    ProcessoTeamTagResponse,
)

from .fluxo import (
    FluxoCreate,
    FluxoUpdate,
    FluxoNodeData,
    FluxoEdgeData,
    FluxoSaveCanvas,
    FluxoProcessoCreate,
    FluxoProcessoUpdate,
    FluxoNodeResponse,
    FluxoEdgeResponse,
    FluxoResponse,
    FluxoDetalheResponse,
    FluxoProcessoResponse,
    FluxoComVinculacaoResponse,
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
    "MoverProcessoKanban",
    "SalvarProcessoKanban",
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
    "ObservacaoUpdate",
    "ObservacaoResponse",
    "ObservacaoMencaoResponse",
    "TeamTagCreate",
    "TeamTagUpdate",
    "TeamTagResponse",
    "ProcessoTeamTagCreate",
    "ProcessoTeamTagResponse",
    "FluxoCreate",
    "FluxoUpdate",
    "FluxoNodeData",
    "FluxoEdgeData",
    "FluxoSaveCanvas",
    "FluxoProcessoCreate",
    "FluxoProcessoUpdate",
    "FluxoNodeResponse",
    "FluxoEdgeResponse",
    "FluxoResponse",
    "FluxoDetalheResponse",
    "FluxoProcessoResponse",
    "FluxoComVinculacaoResponse",
]
