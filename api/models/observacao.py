"""
Model SQLAlchemy para observacoes de processos
"""
from sqlalchemy import Column, String, Text, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from ..database import Base


class Observacao(Base):
    """
    Model para observacoes sobre processos

    Implementa soft delete atraves do campo deletado_em
    """
    __tablename__ = "observacoes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico da observacao"
    )

    numero_processo = Column(
        String(50),
        nullable=False,
        comment="Numero do processo sem formatacao"
    )

    usuario = Column(
        String(100),
        nullable=False,
        comment="Usuario autor da observacao"
    )

    conteudo = Column(
        Text,
        nullable=False,
        comment="Conteudo da observacao"
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
            'idx_observacao_processo',
            'numero_processo',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_observacao_usuario',
            'usuario',
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Tabela de observacoes sobre processos'}
    )

    def __repr__(self) -> str:
        return (
            f"<Observacao("
            f"id={self.id}, "
            f"numero_processo={self.numero_processo}, "
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
