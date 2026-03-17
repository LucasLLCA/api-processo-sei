"""
Model de Node (etapa) de um Fluxo de Processo
"""
from sqlalchemy import Column, String, Text, Float, ForeignKey, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class FluxoNode(Base):
    __tablename__ = "fluxo_nodes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico do node",
    )

    fluxo_id = Column(
        UUID(as_uuid=True),
        ForeignKey("fluxos.id", ondelete="CASCADE"),
        nullable=False,
        comment="Fluxo ao qual o node pertence",
    )

    node_id = Column(
        String(100),
        nullable=False,
        comment="ID do React Flow (client-side)",
    )

    tipo = Column(
        String(30),
        nullable=False,
        comment="Tipo do node: sei_task, etapa, decisao, inicio, fim, fork, join",
    )

    nome = Column(
        String(300),
        nullable=False,
        comment="Nome/label do node",
    )

    descricao = Column(
        Text,
        nullable=True,
        comment="Descricao do node",
    )

    sei_task_key = Column(
        String(50),
        nullable=True,
        comment="Chave do TASK_GROUPS para nodes do tipo sei_task",
    )

    responsavel = Column(
        String(200),
        nullable=True,
        comment="Responsavel pela etapa",
    )

    duracao_estimada_horas = Column(
        Float,
        nullable=True,
        comment="Duracao estimada em horas",
    )

    prioridade = Column(
        String(20),
        nullable=True,
        comment="Prioridade: baixa, media, alta, critica",
    )

    documentos_necessarios = Column(
        JSONB,
        nullable=True,
        comment="Lista de documentos necessarios",
    )

    checklist = Column(
        JSONB,
        nullable=True,
        comment="Lista de checklist items",
    )

    regras_prazo = Column(
        JSONB,
        nullable=True,
        comment="Regras de prazo {dias_uteis, tipo}",
    )

    metadata_extra = Column(
        JSONB,
        nullable=True,
        comment="Metadados arbitrarios key-value",
    )

    posicao_x = Column(
        Float,
        nullable=False,
        comment="Posicao X no canvas",
    )

    posicao_y = Column(
        Float,
        nullable=False,
        comment="Posicao Y no canvas",
    )

    largura = Column(
        Float,
        nullable=True,
        comment="Largura do node no canvas",
    )

    altura = Column(
        Float,
        nullable=True,
        comment="Altura do node no canvas",
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
    fluxo = relationship("Fluxo", back_populates="nodes")

    __table_args__ = (
        Index(
            "uq_fluxo_node_id",
            "fluxo_id",
            "node_id",
            unique=True,
            postgresql_where=text("deletado_em IS NULL"),
        ),
        Index(
            "idx_fluxo_node_fluxo",
            "fluxo_id",
            postgresql_where=text("deletado_em IS NULL"),
        ),
        {"comment": "Nodes (etapas) de um fluxo de processo"},
    )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None

    def __repr__(self) -> str:
        return f"<FluxoNode(id={self.id}, node_id='{self.node_id}', tipo='{self.tipo}')>"
