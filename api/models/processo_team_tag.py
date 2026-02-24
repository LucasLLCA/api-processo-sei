"""
Model SQLAlchemy para associacao processo <-> team tag
"""
from sqlalchemy import Column, String, TIMESTAMP, Index, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class ProcessoTeamTag(Base):
    """
    Model para associacao entre processos e tags de equipe.

    Implementa soft delete atraves do campo deletado_em.
    """
    __tablename__ = "processo_team_tags"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico da associacao"
    )

    team_tag_id = Column(
        UUID(as_uuid=True),
        ForeignKey("team_tags.id", ondelete="CASCADE"),
        nullable=False,
        comment="ID da team tag"
    )

    numero_processo = Column(
        String(50),
        nullable=False,
        comment="Numero do processo sem formatacao"
    )

    adicionado_por = Column(
        String(100),
        nullable=False,
        comment="Usuario que associou a tag ao processo"
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criacao"
    )

    deletado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora da exclusao (soft delete)"
    )

    team_tag = relationship("TeamTag", back_populates="processos")

    __table_args__ = (
        Index(
            'uq_processo_team_tag',
            'team_tag_id', 'numero_processo',
            unique=True,
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_processo_team_tag_numero',
            'numero_processo',
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Associacao entre processos e tags de equipe'}
    )

    def __repr__(self) -> str:
        return (
            f"<ProcessoTeamTag("
            f"id={self.id}, "
            f"team_tag_id={self.team_tag_id}, "
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
