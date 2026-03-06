"""
Model SQLAlchemy para situação atual de processos gerada por IA
"""
from sqlalchemy import Column, String, Integer, Text, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from ..database import Base


class ProcessoSituacao(Base):
    """
    Situação atual de processos gerada por IA.
    Tracked by total_andamentos — regenerated when TotalItens changes.
    Implements soft delete via deletado_em.
    """
    __tablename__ = "processo_situacoes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico",
    )

    numero_processo = Column(
        String(50),
        nullable=False,
        comment="Numero do processo sem formatacao",
    )

    total_andamentos = Column(
        Integer,
        nullable=False,
        comment="TotalItens de andamentos no momento da geracao",
    )

    ultimo_andamento_id = Column(
        String(50),
        nullable=True,
        comment="ID do ultimo andamento como referencia",
    )

    conteudo = Column(
        Text,
        nullable=False,
        comment="Texto da situacao atual gerada por IA",
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criacao",
    )

    atualizado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora da ultima atualizacao",
    )

    deletado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora da exclusao (soft delete)",
    )

    __table_args__ = (
        Index(
            'idx_situacao_processo_unique',
            'numero_processo',
            unique=True,
            postgresql_where=text("deletado_em IS NULL"),
        ),
        {'comment': 'Situacao atual de processos gerada por IA'},
    )

    def __repr__(self) -> str:
        return (
            f"<ProcessoSituacao("
            f"id={self.id}, "
            f"numero_processo={self.numero_processo}, "
            f"total_andamentos={self.total_andamentos}"
            f")>"
        )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
