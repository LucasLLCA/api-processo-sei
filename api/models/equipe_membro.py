"""
Model SQLAlchemy para membros de equipe
"""
from sqlalchemy import Column, String, TIMESTAMP, Index, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class EquipeMembro(Base):
    """
    Model para membros de equipe

    Implementa soft delete através do campo deletado_em
    """
    __tablename__ = "equipe_membros"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador único do membro"
    )

    equipe_id = Column(
        UUID(as_uuid=True),
        ForeignKey("equipes.id", ondelete="CASCADE"),
        nullable=False,
        comment="ID da equipe"
    )

    usuario = Column(
        String(100),
        nullable=False,
        comment="Identificação do usuário membro"
    )

    papel = Column(
        String(20),
        nullable=False,
        server_default=text("'member'"),
        comment="Papel do membro na equipe (admin, member)"
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

    equipe = relationship("Equipe", back_populates="membros")

    __table_args__ = (
        Index(
            'uq_equipe_membro_usuario',
            'equipe_id', 'usuario',
            unique=True,
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_membro_equipe',
            'equipe_id',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_membro_usuario',
            'usuario',
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Tabela de membros de equipe'}
    )

    def __repr__(self) -> str:
        return (
            f"<EquipeMembro("
            f"id={self.id}, "
            f"equipe_id={self.equipe_id}, "
            f"usuario={self.usuario}, "
            f"papel={self.papel}"
            f")>"
        )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
