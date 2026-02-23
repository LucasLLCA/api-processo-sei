"""
Model SQLAlchemy para equipes
"""
from sqlalchemy import Column, String, Text, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class Equipe(Base):
    """
    Model para equipes de trabalho

    Implementa soft delete através do campo deletado_em
    """
    __tablename__ = "equipes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador único da equipe"
    )

    nome = Column(
        String(200),
        nullable=False,
        comment="Nome da equipe"
    )

    descricao = Column(
        Text,
        nullable=True,
        comment="Descrição da equipe"
    )

    proprietario_usuario = Column(
        String(100),
        nullable=False,
        comment="Usuário proprietário da equipe"
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criação"
    )

    atualizado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora da última atualização"
    )

    deletado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora da exclusão (soft delete)"
    )

    membros = relationship("EquipeMembro", back_populates="equipe", lazy="selectin")

    __table_args__ = (
        Index(
            'idx_equipe_proprietario',
            'proprietario_usuario',
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Tabela de equipes de trabalho'}
    )

    def __repr__(self) -> str:
        return (
            f"<Equipe("
            f"id={self.id}, "
            f"nome={self.nome}, "
            f"proprietario={self.proprietario_usuario}"
            f")>"
        )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
