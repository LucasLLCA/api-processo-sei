"""
Model SQLAlchemy para credenciais SEI armazenadas por usuário
"""
from sqlalchemy import Column, String, Text, BigInteger, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from ..database import Base


class CredencialUsuario(Base):
    """
    Credenciais SEI criptografadas vinculadas a um usuário (id_pessoa do JWE).
    Implementa soft delete — apenas uma credencial ativa por id_pessoa.
    """
    __tablename__ = "credenciais_usuario"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )

    id_pessoa = Column(BigInteger, nullable=False)
    cpf = Column(String(20), nullable=True)  # CPF from JWE usuario field
    usuario_sei = Column(String(100), nullable=False)
    senha_encrypted = Column(Text, nullable=False)
    orgao = Column(String(50), nullable=False)
    papel_global = Column(
        String(20),
        nullable=False,
        server_default=text("'user'"),
        comment="Papel global do usuário (admin, beta, user)",
    )

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

    __table_args__ = (
        Index(
            'idx_credencial_id_pessoa_unique',
            'id_pessoa',
            unique=True,
            postgresql_where=text("deletado_em IS NULL"),
        ),
        {'comment': 'Credenciais SEI criptografadas por usuário'},
    )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
