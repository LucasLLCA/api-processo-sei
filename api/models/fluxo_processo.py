"""
Model de vinculacao Processo-Fluxo (process-to-flow assignment)
"""
from sqlalchemy import Column, String, Text, ForeignKey, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class FluxoProcesso(Base):
    __tablename__ = "fluxo_processos"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico da vinculacao",
    )

    fluxo_id = Column(
        UUID(as_uuid=True),
        ForeignKey("fluxos.id", ondelete="CASCADE"),
        nullable=False,
        comment="Fluxo vinculado",
    )

    numero_processo = Column(
        String(50),
        nullable=False,
        comment="Numero do processo (sem formatacao)",
    )

    numero_processo_formatado = Column(
        String(50),
        nullable=True,
        comment="Numero do processo formatado",
    )

    node_atual_id = Column(
        String(100),
        nullable=True,
        comment="node_id da etapa atual do processo no fluxo",
    )

    status = Column(
        String(30),
        nullable=False,
        default="em_andamento",
        server_default=text("'em_andamento'"),
        comment="Status: em_andamento, concluido, pausado, cancelado",
    )

    iniciado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora de inicio do processo no fluxo",
    )

    concluido_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora de conclusao",
    )

    atribuido_por = Column(
        String(100),
        nullable=False,
        comment="Usuario que atribuiu o processo ao fluxo",
    )

    notas = Column(
        Text,
        nullable=True,
        comment="Notas sobre a vinculacao",
    )

    historico = Column(
        JSONB,
        nullable=True,
        default=list,
        server_default=text("'[]'::jsonb"),
        comment="Historico de movimentacoes [{node_id, entrada_em, saida_em, usuario}]",
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criacao",
    )

    atualizado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora da ultima atualizacao",
    )

    deletado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora da exclusao (soft delete)",
    )

    # Relationships
    fluxo = relationship("Fluxo", back_populates="processos_vinculados")

    __table_args__ = (
        Index(
            "uq_fluxo_processo",
            "fluxo_id",
            "numero_processo",
            unique=True,
            postgresql_where=text("deletado_em IS NULL"),
        ),
        Index(
            "idx_fluxo_processo_fluxo",
            "fluxo_id",
            postgresql_where=text("deletado_em IS NULL"),
        ),
        Index(
            "idx_fluxo_processo_numero",
            "numero_processo",
            postgresql_where=text("deletado_em IS NULL"),
        ),
        {"comment": "Vinculacao de processos a fluxos"},
    )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None

    def __repr__(self) -> str:
        return f"<FluxoProcesso(id={self.id}, fluxo_id={self.fluxo_id}, processo='{self.numero_processo}')>"
