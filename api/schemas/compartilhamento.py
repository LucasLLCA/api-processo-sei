"""
Schemas Pydantic para compartilhamentos
"""
from pydantic import BaseModel, Field, ConfigDict, model_validator
from datetime import datetime
from typing import Optional
from uuid import UUID


class CompartilhamentoCreate(BaseModel):
    tag_id: UUID = Field(..., description="ID da tag a compartilhar")
    equipe_destino_id: Optional[UUID] = Field(None, description="ID da equipe destino")
    usuario_destino: Optional[str] = Field(None, max_length=100, description="Usuário destino")

    @model_validator(mode='after')
    def validate_destino(self):
        if self.equipe_destino_id and self.usuario_destino:
            raise ValueError("Defina apenas equipe_destino_id ou usuario_destino, não ambos")
        if not self.equipe_destino_id and not self.usuario_destino:
            raise ValueError("Defina equipe_destino_id ou usuario_destino")
        return self


class CompartilhamentoResponse(BaseModel):
    id: UUID
    tag_id: UUID
    compartilhado_por: str
    equipe_destino_id: Optional[UUID] = None
    usuario_destino: Optional[str] = None
    criado_em: datetime

    model_config = ConfigDict(from_attributes=True)


class CompartilhadoComMigoItem(BaseModel):
    """Item na listagem 'Compartilhados comigo'"""
    compartilhamento_id: UUID
    tag_id: UUID
    tag_nome: str
    tag_cor: Optional[str] = None
    compartilhado_por: str
    equipe_nome: Optional[str] = None
    criado_em: datetime
    processos: list = []
