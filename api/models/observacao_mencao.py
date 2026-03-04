"""
Model SQLAlchemy para mencoes em observacoes
"""
from sqlalchemy import Column, String, TIMESTAMP, ForeignKey, Index, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class ObservacaoMencao(Base):
    """
    Registra quando um usuario e mencionado em uma observacao.
    visto_em == None  → nao visualizado ainda (gera badge de notificacao)
    visto_em != None  → ja foi visto
    """
    __tablename__ = "observacao_mencoes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico da mencao"
    )

    observacao_id = Column(
        UUID(as_uuid=True),
        ForeignKey("observacoes.id", ondelete="CASCADE"),
        nullable=False,
        comment="Observacao onde a mencao foi feita"
    )

    usuario_mencionado = Column(
        String(100),
        nullable=False,
        comment="Usuario mencionado"
    )

    visto_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora em que o usuario mencionado visualizou"
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora da mencao"
    )

    # Relationship
    observacao = relationship("Observacao", back_populates="mencoes", foreign_keys=[observacao_id])

    __table_args__ = (
        Index('idx_mencao_observacao', 'observacao_id'),
        Index(
            'idx_mencao_usuario_nao_visto',
            'usuario_mencionado',
            postgresql_where=text("visto_em IS NULL")
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<ObservacaoMencao("
            f"observacao_id={self.observacao_id}, "
            f"usuario={self.usuario_mencionado}, "
            f"visto={self.visto_em is not None}"
            f")>"
        )

    def marcar_visto(self) -> None:
        self.visto_em = datetime.utcnow()

    @property
    def foi_visto(self) -> bool:
        return self.visto_em is not None
