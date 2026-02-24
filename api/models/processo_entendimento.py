"""
Model SQLAlchemy para entendimentos de processos gerados por IA
"""
from sqlalchemy import Column, String, Text, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from ..database import Base


class ProcessoEntendimento(Base):
    """
    Model para entendimentos de processos gerados por IA.

    Implementa soft delete atraves do campo deletado_em.
    """
    __tablename__ = "processo_entendimentos"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico do entendimento"
    )

    numero_processo = Column(
        String(50),
        nullable=False,
        comment="Numero do processo sem formatacao"
    )

    conteudo = Column(
        Text,
        nullable=False,
        comment="Conteudo do entendimento gerado por IA"
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criacao"
    )

    atualizado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora da ultima atualizacao"
    )

    deletado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora da exclusao (soft delete)"
    )

    __table_args__ = (
        Index(
            'idx_entendimento_processo_unique',
            'numero_processo',
            unique=True,
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Entendimentos de processos gerados por IA'}
    )

    def __repr__(self) -> str:
        return (
            f"<ProcessoEntendimento("
            f"id={self.id}, "
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
