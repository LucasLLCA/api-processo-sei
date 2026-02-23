"""
Model SQLAlchemy para processos salvos (junção tag ↔ processo)
"""
from sqlalchemy import Column, String, Text, TIMESTAMP, Index, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class ProcessoSalvo(Base):
    """
    Model para processos salvos dentro de tags

    Implementa soft delete através do campo deletado_em
    """
    __tablename__ = "processos_salvos"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador único do processo salvo"
    )

    tag_id = Column(
        UUID(as_uuid=True),
        ForeignKey("tags.id", ondelete="CASCADE"),
        nullable=False,
        comment="ID da tag associada"
    )

    numero_processo = Column(
        String(50),
        nullable=False,
        comment="Número do processo sem formatação"
    )

    numero_processo_formatado = Column(
        String(50),
        nullable=True,
        comment="Número do processo formatado"
    )

    nota = Column(
        Text,
        nullable=True,
        comment="Nota opcional sobre o processo"
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criação"
    )

    deletado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora da exclusão (soft delete)"
    )

    tag = relationship("Tag", back_populates="processos")

    __table_args__ = (
        Index(
            'uq_processo_salvo_tag_numero',
            'tag_id', 'numero_processo',
            unique=True,
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_processo_salvo_tag',
            'tag_id',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_processo_salvo_numero',
            'numero_processo',
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Tabela de processos salvos em tags'}
    )

    def __repr__(self) -> str:
        return (
            f"<ProcessoSalvo("
            f"id={self.id}, "
            f"tag_id={self.tag_id}, "
            f"numero_processo={self.numero_processo}"
            f")>"
        )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
