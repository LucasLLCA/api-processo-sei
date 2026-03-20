"""
Model de Unidade SEI (caixa/unidade organizacional)
"""
from sqlalchemy import Column, String, BigInteger, TIMESTAMP, Index, text

from ..database import Base


class UnidadeSei(Base):
    __tablename__ = "unidades_sei"

    id_unidade = Column(
        BigInteger,
        primary_key=True,
        comment="ID da unidade no SEI",
    )

    sigla = Column(
        String(300),
        nullable=False,
        comment="Sigla/código da unidade (ex: SEAD-PI/GAB)",
    )

    descricao = Column(
        String(500),
        nullable=False,
        comment="Descrição completa da unidade",
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criação",
    )

    __table_args__ = (
        Index("idx_unidade_sei_sigla", "sigla"),
        {"comment": "Unidades organizacionais do SEI"},
    )

    def __repr__(self) -> str:
        return f"<UnidadeSei(id_unidade={self.id_unidade}, sigla='{self.sigla}')>"
