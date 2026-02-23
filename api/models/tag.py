"""
Model SQLAlchemy para tags de processos (agrupamento por usuário)
"""
from sqlalchemy import Column, String, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class Tag(Base):
    """
    Model para tags de processos salvos (escopadas por usuário)

    Implementa soft delete através do campo deletado_em
    """
    __tablename__ = "tags"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador único da tag"
    )

    nome = Column(
        String(200),
        nullable=False,
        comment="Nome da tag"
    )

    usuario = Column(
        String(100),
        nullable=False,
        comment="Usuário proprietário da tag"
    )

    cor = Column(
        String(7),
        nullable=True,
        comment="Cor hex da tag (ex: #3B82F6)"
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

    processos = relationship("ProcessoSalvo", back_populates="tag", lazy="selectin")
    compartilhamentos = relationship("Compartilhamento", back_populates="tag", lazy="selectin")

    __table_args__ = (
        Index(
            'uq_tag_usuario_nome',
            'usuario', 'nome',
            unique=True,
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_tag_usuario',
            'usuario',
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Tabela de tags para agrupamento de processos salvos'}
    )

    def __repr__(self) -> str:
        return (
            f"<Tag("
            f"id={self.id}, "
            f"nome={self.nome}, "
            f"usuario={self.usuario}"
            f")>"
        )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
