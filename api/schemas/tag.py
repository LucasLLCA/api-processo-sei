"""
Schemas Pydantic para tags e processos salvos
"""
import re
from pydantic import BaseModel, Field, ConfigDict, field_validator
from datetime import datetime
from typing import Optional
from uuid import UUID


class TagCreate(BaseModel):
    nome: str = Field(..., min_length=1, max_length=200, description="Nome da tag")
    cor: Optional[str] = Field(None, max_length=7, description="Cor hex da tag (ex: #3B82F6)")


class TagUpdate(BaseModel):
    nome: Optional[str] = Field(None, min_length=1, max_length=200)
    cor: Optional[str] = Field(None, max_length=7)


class TagResponse(BaseModel):
    id: UUID
    nome: str
    usuario: str
    cor: Optional[str] = None
    criado_em: datetime
    atualizado_em: datetime
    total_processos: int = 0

    model_config = ConfigDict(from_attributes=True)


class ProcessoSalvoCreate(BaseModel):
    numero_processo: str = Field(..., min_length=1, max_length=50, description="Número do processo")
    numero_processo_formatado: Optional[str] = Field(None, max_length=50)
    nota: Optional[str] = Field(None, description="Nota opcional sobre o processo")

    @field_validator('numero_processo', mode='before')
    @classmethod
    def strip_non_digits(cls, v: str) -> str:
        return re.sub(r'\D', '', v)


class ProcessoSalvoResponse(BaseModel):
    id: UUID
    tag_id: UUID
    numero_processo: str
    numero_processo_formatado: Optional[str] = None
    nota: Optional[str] = None
    criado_em: datetime

    model_config = ConfigDict(from_attributes=True)


class TagComProcessosResponse(BaseModel):
    id: UUID
    nome: str
    usuario: str
    cor: Optional[str] = None
    criado_em: datetime
    atualizado_em: datetime
    processos: list[ProcessoSalvoResponse] = []

    model_config = ConfigDict(from_attributes=True)
