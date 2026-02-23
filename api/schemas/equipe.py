"""
Schemas Pydantic para equipes
"""
from pydantic import BaseModel, Field, ConfigDict
from datetime import datetime
from typing import Optional
from uuid import UUID


class EquipeCreate(BaseModel):
    nome: str = Field(..., min_length=1, max_length=200, description="Nome da equipe")
    descricao: Optional[str] = Field(None, description="Descrição da equipe")


class EquipeUpdate(BaseModel):
    nome: Optional[str] = Field(None, min_length=1, max_length=200, description="Novo nome da equipe")
    descricao: Optional[str] = Field(None, description="Nova descrição da equipe")


class MembroAdd(BaseModel):
    usuario: str = Field(..., min_length=1, max_length=100, description="Email/identificação do usuário a adicionar")
    papel: str = Field("member", description="Papel do membro (admin, member)")


class MembroResponse(BaseModel):
    id: UUID
    equipe_id: UUID
    usuario: str
    papel: str
    criado_em: datetime

    model_config = ConfigDict(from_attributes=True)


class EquipeResponse(BaseModel):
    id: UUID
    nome: str
    descricao: Optional[str] = None
    proprietario_usuario: str
    criado_em: datetime
    atualizado_em: datetime
    total_membros: int = 0

    model_config = ConfigDict(from_attributes=True)


class EquipeDetalheResponse(BaseModel):
    id: UUID
    nome: str
    descricao: Optional[str] = None
    proprietario_usuario: str
    criado_em: datetime
    atualizado_em: datetime
    membros: list[MembroResponse] = []

    model_config = ConfigDict(from_attributes=True)
