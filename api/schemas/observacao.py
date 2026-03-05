"""
Schemas Pydantic para observacoes de processos
"""
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional, Literal, List
from uuid import UUID


class ObservacaoCreate(BaseModel):
    conteudo: str = Field(..., min_length=1, max_length=5000, description="Conteudo da observacao")
    escopo: Literal['pessoal', 'equipe', 'global'] = Field(
        'global',
        description="Escopo: pessoal (so o autor ve), equipe (membros da equipe), global (todos)",
    )
    equipe_id: Optional[UUID] = Field(
        None,
        description="ID da equipe — obrigatorio quando escopo='equipe'",
    )
    parent_id: Optional[UUID] = Field(
        None,
        description="ID da observacao pai — quando for uma resposta",
    )
    mencoes: List[str] = Field(
        default_factory=list,
        description="Lista de usuarios mencionados (@usuario extraidos do conteudo)",
    )


class ObservacaoMencaoResponse(BaseModel):
    id: UUID
    observacao_id: UUID
    usuario_mencionado: str
    visto_em: Optional[datetime] = None
    criado_em: datetime

    model_config = ConfigDict(from_attributes=True)


class ObservacaoResponse(BaseModel):
    id: UUID
    numero_processo: str
    usuario: str
    conteudo: str
    escopo: str = 'global'
    equipe_id: Optional[UUID] = None
    parent_id: Optional[UUID] = None
    mencoes: List[ObservacaoMencaoResponse] = []
    respostas: List['ObservacaoResponse'] = []
    criado_em: datetime
    atualizado_em: datetime

    model_config = ConfigDict(from_attributes=True)


# Necessario para referencias circulares (respostas dentro de ObservacaoResponse)
ObservacaoResponse.model_rebuild()
