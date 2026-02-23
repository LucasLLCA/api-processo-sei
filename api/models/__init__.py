"""
Models do banco de dados
"""
from .historico_pesquisa import HistoricoPesquisa
from .equipe import Equipe
from .equipe_membro import EquipeMembro
from .tag import Tag
from .processo_salvo import ProcessoSalvo
from .compartilhamento import Compartilhamento
from .observacao import Observacao

__all__ = [
    "HistoricoPesquisa",
    "Equipe",
    "EquipeMembro",
    "Tag",
    "ProcessoSalvo",
    "Compartilhamento",
    "Observacao",
]
