"""
Model SQLAlchemy para tags de equipe (rotulos no kanban)
"""
from sqlalchemy import Column, String, TIMESTAMP, Index, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class TeamTag(Base):
    """
    Model para tags de equipe — rotulos coloridos aplicaveis a processos no kanban.

    Escopadas por equipe. Qualquer membro pode criar.
    Implementa soft delete atraves do campo deletado_em.
    """
    __tablename__ = "team_tags"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico da team tag"
    )

    equipe_id = Column(
        UUID(as_uuid=True),
        ForeignKey("equipes.id", ondelete="CASCADE"),
        nullable=False,
        comment="ID da equipe dona da tag"
    )

    nome = Column(
        String(100),
        nullable=False,
        comment="Nome da tag"
    )

    cor = Column(
        String(7),
        nullable=True,
        comment="Cor hex da tag (ex: #3B82F6)"
    )

    criado_por = Column(
        String(100),
        nullable=False,
        comment="Usuario que criou a tag"
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

    processos = relationship("ProcessoTeamTag", back_populates="team_tag", lazy="selectin")

    __table_args__ = (
        Index(
            'uq_team_tag_equipe_nome',
            'equipe_id', 'nome',
            unique=True,
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_team_tag_equipe',
            'equipe_id',
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Tags de equipe para rotular processos no kanban'}
    )

    def __repr__(self) -> str:
        return (
            f"<TeamTag("
            f"id={self.id}, "
            f"equipe_id={self.equipe_id}, "
            f"nome={self.nome}"
            f")>"
        )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
