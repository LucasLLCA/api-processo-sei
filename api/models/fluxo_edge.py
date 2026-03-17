"""
Model de Edge (conexao) de um Fluxo de Processo
"""
from sqlalchemy import Column, String, Integer, Boolean, ForeignKey, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class FluxoEdge(Base):
    __tablename__ = "fluxo_edges"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico da edge",
    )

    fluxo_id = Column(
        UUID(as_uuid=True),
        ForeignKey("fluxos.id", ondelete="CASCADE"),
        nullable=False,
        comment="Fluxo ao qual a edge pertence",
    )

    edge_id = Column(
        String(100),
        nullable=False,
        comment="ID do React Flow (client-side)",
    )

    source_node_id = Column(
        String(100),
        nullable=False,
        comment="node_id de origem",
    )

    target_node_id = Column(
        String(100),
        nullable=False,
        comment="node_id de destino",
    )

    tipo = Column(
        String(30),
        nullable=False,
        default="padrao",
        server_default=text("'padrao'"),
        comment="Tipo da edge: padrao, condicional, loop",
    )

    label = Column(
        String(200),
        nullable=True,
        comment="Texto de exibicao na edge",
    )

    condicao = Column(
        JSONB,
        nullable=True,
        comment="Condicao para edges condicionais {campo, operador, valor}",
    )

    ordem = Column(
        Integer,
        nullable=True,
        comment="Ordem das edges de saida de um node de decisao",
    )

    animated = Column(
        Boolean,
        nullable=False,
        default=False,
        server_default=text("false"),
        comment="Se a edge deve ser animada",
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
    fluxo = relationship("Fluxo", back_populates="edges")

    __table_args__ = (
        Index(
            "uq_fluxo_edge_id",
            "fluxo_id",
            "edge_id",
            unique=True,
            postgresql_where=text("deletado_em IS NULL"),
        ),
        Index(
            "idx_fluxo_edge_fluxo",
            "fluxo_id",
            postgresql_where=text("deletado_em IS NULL"),
        ),
        {"comment": "Edges (conexoes) de um fluxo de processo"},
    )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None

    def __repr__(self) -> str:
        return f"<FluxoEdge(id={self.id}, edge_id='{self.edge_id}', {self.source_node_id}->{self.target_node_id})>"
