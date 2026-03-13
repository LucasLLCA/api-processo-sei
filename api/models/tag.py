"""
Model SQLAlchemy para grupos de processos.

Grupos podem ser pessoais (equipe_id IS NULL) ou de equipe (equipe_id set).
"""
from sqlalchemy import Column, String, TIMESTAMP, Index, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class Tag(Base):
    """
    Model para grupos de processos salvos.

    Quando equipe_id IS NULL: grupo pessoal, visivel apenas para usuario.
    Quando equipe_id IS NOT NULL: grupo de equipe, visivel para membros.
    Implementa soft delete atraves do campo deletado_em.
    """
    __tablename__ = "tags"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico do grupo"
    )

    nome = Column(
        String(200),
        nullable=False,
        comment="Nome do grupo"
    )

    usuario = Column(
        String(100),
        nullable=False,
        comment="Usuario que criou o grupo"
    )

    equipe_id = Column(
        UUID(as_uuid=True),
        ForeignKey("equipes.id", ondelete="CASCADE"),
        nullable=True,
        comment="ID da equipe dona do grupo (NULL = grupo pessoal)"
    )

    cor = Column(
        String(7),
        nullable=True,
        comment="Cor hex do grupo (ex: #3B82F6)"
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

    processos = relationship("ProcessoSalvo", back_populates="tag", lazy="selectin")

    __table_args__ = (
        Index(
            'uq_tag_usuario_nome',
            'usuario', 'nome',
            unique=True,
            postgresql_where=text("deletado_em IS NULL AND equipe_id IS NULL")
        ),
        Index(
            'uq_tags_equipe_nome',
            'equipe_id', 'nome',
            unique=True,
            postgresql_where=text("deletado_em IS NULL AND equipe_id IS NOT NULL")
        ),
        Index(
            'idx_tag_usuario',
            'usuario',
            postgresql_where=text("deletado_em IS NULL AND equipe_id IS NULL")
        ),
        Index(
            'idx_tags_equipe_id',
            'equipe_id',
            postgresql_where=text("deletado_em IS NULL AND equipe_id IS NOT NULL")
        ),
        {'comment': 'Tabela de grupos de processos (pessoais ou de equipe)'}
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
