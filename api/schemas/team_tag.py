"""
Schemas Pydantic para team tags e processo_team_tags
"""
import re
from pydantic import BaseModel, Field, ConfigDict, field_validator
from datetime import datetime
from typing import Optional
from uuid import UUID


class TeamTagCreate(BaseModel):
    nome: str = Field(..., min_length=1, max_length=100, description="Nome da tag")
    cor: Optional[str] = Field(None, max_length=7, description="Cor hex da tag (ex: #3B82F6)")


class TeamTagUpdate(BaseModel):
    nome: Optional[str] = Field(None, min_length=1, max_length=100)
    cor: Optional[str] = Field(None, max_length=7)


class TeamTagResponse(BaseModel):
    id: UUID
    equipe_id: UUID
    nome: str
    cor: Optional[str] = None
    criado_por: str
    criado_em: datetime
    atualizado_em: datetime

    model_config = ConfigDict(from_attributes=True)


class ProcessoTeamTagCreate(BaseModel):
    numero_processo: str = Field(..., min_length=1, max_length=50, description="Numero do processo")

    @field_validator('numero_processo', mode='before')
    @classmethod
    def strip_non_digits(cls, v: str) -> str:
        return re.sub(r'\D', '', v)


class ProcessoTeamTagResponse(BaseModel):
    id: UUID
    team_tag_id: UUID
    numero_processo: str
    adicionado_por: str
    criado_em: datetime

    model_config = ConfigDict(from_attributes=True)
