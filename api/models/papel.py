"""
Model SQLAlchemy para papéis (roles) do sistema RBAC.
"""
from datetime import datetime
import uuid

from sqlalchemy import Column, String, Text, Boolean, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import relationship

from ..database import Base


class Papel(Base):
    """
    Define um papel (role) com lista de módulos permitidos.
    Soft delete — apenas um papel ativo por slug.
    """
    __tablename__ = "papeis"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )

    nome = Column(String(60), nullable=False)
    slug = Column(String(40), nullable=False)
    descricao = Column(Text, nullable=True)
    modulos = Column(ARRAY(Text), nullable=False, server_default=text("'{}'"))
    is_default = Column(Boolean, nullable=False, server_default=text("false"))

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
    usuarios = relationship("UsuarioPapel", back_populates="papel")

    __table_args__ = (
        Index(
            "idx_papel_slug_unique",
            "slug",
            unique=True,
            postgresql_where=text("deletado_em IS NULL"),
        ),
        {"comment": "Papéis (roles) do sistema com módulos permitidos"},
    )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
