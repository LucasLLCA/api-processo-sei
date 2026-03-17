"""
Model de Fluxo de Processo (workflow template)
"""
from sqlalchemy import Column, String, Text, Integer, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class Fluxo(Base):
    __tablename__ = "fluxos"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico do fluxo",
    )

    nome = Column(
        String(200),
        nullable=False,
        comment="Nome do fluxo",
    )

    descricao = Column(
        Text,
        nullable=True,
        comment="Descricao do fluxo",
    )

    usuario = Column(
        String(100),
        nullable=False,
        comment="Usuario criador do fluxo",
    )

    equipe_id = Column(
        UUID(as_uuid=True),
        nullable=True,
        comment="Equipe dona do fluxo (NULL = pessoal)",
    )

    orgao = Column(
        String(50),
        nullable=True,
        comment="Orgao para fluxo organizacional (NULL = nao-org)",
    )

    versao = Column(
        Integer,
        nullable=False,
        default=1,
        server_default=text("1"),
        comment="Versao para controle de concorrencia otimista",
    )

    status = Column(
        String(20),
        nullable=False,
        default="rascunho",
        server_default=text("'rascunho'"),
        comment="Status do fluxo: rascunho, publicado, arquivado",
    )

    viewport = Column(
        JSONB,
        nullable=True,
        comment="Viewport do canvas {x, y, zoom}",
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
    nodes = relationship(
        "FluxoNode",
        back_populates="fluxo",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    edges = relationship(
        "FluxoEdge",
        back_populates="fluxo",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    processos_vinculados = relationship(
        "FluxoProcesso",
        back_populates="fluxo",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index(
            "idx_fluxo_usuario",
            "usuario",
            postgresql_where=text("deletado_em IS NULL"),
        ),
        Index(
            "idx_fluxo_equipe",
            "equipe_id",
            postgresql_where=text("deletado_em IS NULL AND equipe_id IS NOT NULL"),
        ),
        Index(
            "idx_fluxo_orgao",
            "orgao",
            postgresql_where=text("deletado_em IS NULL AND orgao IS NOT NULL"),
        ),
        {"comment": "Fluxos de processos (workflow templates)"},
    )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None

    def __repr__(self) -> str:
        return f"<Fluxo(id={self.id}, nome='{self.nome}', status='{self.status}')>"
