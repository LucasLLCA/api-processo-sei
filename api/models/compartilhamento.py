"""
Model SQLAlchemy para compartilhamentos de tags
"""
from sqlalchemy import Column, String, TIMESTAMP, Index, ForeignKey, CheckConstraint, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class Compartilhamento(Base):
    """
    Model para compartilhamento de tags com usuários ou equipes

    CHECK constraint garante que exatamente um destino está definido.
    Implementa soft delete através do campo deletado_em.
    """
    __tablename__ = "compartilhamentos"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador único do compartilhamento"
    )

    tag_id = Column(
        UUID(as_uuid=True),
        ForeignKey("tags.id", ondelete="CASCADE"),
        nullable=False,
        comment="ID da tag compartilhada"
    )

    compartilhado_por = Column(
        String(100),
        nullable=False,
        comment="Usuário que compartilhou"
    )

    equipe_destino_id = Column(
        UUID(as_uuid=True),
        ForeignKey("equipes.id", ondelete="CASCADE"),
        nullable=True,
        comment="ID da equipe destino (se compartilhamento com equipe)"
    )

    usuario_destino = Column(
        String(100),
        nullable=True,
        comment="Usuário destino (se compartilhamento individual)"
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

    tag = relationship("Tag", back_populates="compartilhamentos")
    equipe_destino = relationship("Equipe", foreign_keys=[equipe_destino_id])

    __table_args__ = (
        CheckConstraint(
            "(equipe_destino_id IS NOT NULL AND usuario_destino IS NULL) OR "
            "(equipe_destino_id IS NULL AND usuario_destino IS NOT NULL)",
            name="ck_compartilhamento_destino_exclusivo"
        ),
        Index(
            'idx_compartilhamento_tag',
            'tag_id',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_compartilhamento_usuario_destino',
            'usuario_destino',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_compartilhamento_equipe_destino',
            'equipe_destino_id',
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Tabela de compartilhamentos de tags'}
    )

    def __repr__(self) -> str:
        destino = self.usuario_destino or f"equipe:{self.equipe_destino_id}"
        return (
            f"<Compartilhamento("
            f"id={self.id}, "
            f"tag_id={self.tag_id}, "
            f"por={self.compartilhado_por}, "
            f"para={destino}"
            f")>"
        )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
