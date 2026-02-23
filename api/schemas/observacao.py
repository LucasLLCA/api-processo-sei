"""
Schemas Pydantic para observacoes de processos
"""
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from uuid import UUID


class ObservacaoCreate(BaseModel):
    conteudo: str = Field(..., min_length=1, max_length=5000, description="Conteudo da observacao")


class ObservacaoResponse(BaseModel):
    id: UUID
    numero_processo: str
    usuario: str
    conteudo: str
    criado_em: datetime
    atualizado_em: datetime

    model_config = ConfigDict(from_attributes=True)
