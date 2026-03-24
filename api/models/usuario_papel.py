"""
Model SQLAlchemy para atribuição de papel por usuario_sei (email SEI).
Papel é vinculado ao email, não ao id_pessoa — usuarios com mesmo email compartilham o papel.
"""
from datetime import datetime
import uuid

from sqlalchemy import Column, String, TIMESTAMP, Index, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from ..database import Base


class UsuarioPapel(Base):
    """
    Vincula um usuario_sei (email SEI) a um papel.
    Apenas uma atribuição ativa por usuario_sei (soft delete aware).
    """
    __tablename__ = "usuario_papel"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )

    usuario_sei = Column(String(100), nullable=False)
    papel_id = Column(
        UUID(as_uuid=True),
        ForeignKey("papeis.id", ondelete="RESTRICT"),
        nullable=False,
    )
    atribuido_por = Column(String(100), nullable=True)

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    atualizado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
    )
    deletado_em = Column(TIMESTAMP(timezone=True), nullable=True)

    # Relationship
    papel = relationship("Papel", back_populates="usuarios")

    __table_args__ = (
        Index(
            "idx_usuario_papel_usuario_unique",
            "usuario_sei",
            unique=True,
            postgresql_where=text("deletado_em IS NULL"),
        ),
        Index(
            "idx_usuario_papel_papel_id",
            "papel_id",
            postgresql_where=text("deletado_em IS NULL"),
        ),
        {"comment": "Atribuição de papel por usuario_sei (compartilhado entre id_pessoa)"},
    )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
