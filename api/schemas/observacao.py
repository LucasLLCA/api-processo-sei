"""
Schemas Pydantic para observacoes de processos
"""
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional
from uuid import UUID


class ObservacaoCreate(BaseModel):
    conteudo: str = Field(..., min_length=1, max_length=5000, description="Conteudo da observacao")
    equipe_id: Optional[UUID] = Field(None, description="ID da equipe (NULL = global)")


class ObservacaoResponse(BaseModel):
    id: UUID
    numero_processo: str
    usuario: str
    conteudo: str
    equipe_id: Optional[UUID] = None
    criado_em: datetime
    atualizado_em: datetime

    model_config = ConfigDict(from_attributes=True)
