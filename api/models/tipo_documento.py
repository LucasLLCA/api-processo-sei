"""
Model de Tipo de Documento SEI
"""
from sqlalchemy import Column, String, BigInteger, TIMESTAMP, Index, text

from ..database import Base


class TipoDocumento(Base):
    __tablename__ = "tipos_documento"

    id_tipo_documento = Column(
        BigInteger,
        primary_key=True,
        comment="ID do tipo de documento no SEI",
    )

    nome = Column(
        String(500),
        nullable=False,
        comment="Nome do tipo de documento",
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criação",
    )

    __table_args__ = (
        Index("idx_tipo_documento_nome", "nome"),
        {"comment": "Tipos de documento do SEI"},
    )

    def __repr__(self) -> str:
        return f"<TipoDocumento(id={self.id_tipo_documento}, nome='{self.nome}')>"
