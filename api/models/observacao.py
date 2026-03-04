"""
Model SQLAlchemy para observacoes de processos
"""
from sqlalchemy import Column, String, Text, TIMESTAMP, Index, ForeignKey, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from datetime import datetime
import uuid

from ..database import Base


class Observacao(Base):
    """
    Model para observacoes sobre processos

    Implementa soft delete atraves do campo deletado_em.
    Suporta respostas via parent_id (auto-referencia).
    Suporta mencoes via relationship com ObservacaoMencao.
    """
    __tablename__ = "observacoes"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico da observacao"
    )

    numero_processo = Column(
        String(50),
        nullable=False,
        comment="Numero do processo sem formatacao"
    )

    usuario = Column(
        String(100),
        nullable=False,
        comment="Usuario autor da observacao"
    )

    escopo = Column(
        String(10),
        nullable=False,
        default='global',
        server_default='global',
        comment="Escopo da observacao: pessoal | equipe | global"
    )

    equipe_id = Column(
        UUID(as_uuid=True),
        ForeignKey("equipes.id", ondelete="SET NULL"),
        nullable=True,
        comment="ID da equipe (obrigatorio quando escopo=equipe)"
    )

    parent_id = Column(
        UUID(as_uuid=True),
        ForeignKey("observacoes.id", ondelete="SET NULL"),
        nullable=True,
        comment="ID da observacao pai (quando for uma resposta)"
    )

    conteudo = Column(
        Text,
        nullable=False,
        comment="Conteudo da observacao"
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criacao"
    )

    atualizado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora da ultima atualizacao"
    )

    deletado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora da exclusao (soft delete)"
    )

    # Respostas: obs filhas que apontam para esta como parent
    respostas = relationship(
        "Observacao",
        primaryjoin="and_(Observacao.parent_id == foreign(Observacao.id), Observacao.deletado_em == None)",
        order_by="Observacao.criado_em",
        lazy="selectin",
        uselist=True,
    )

    # Mencoes: usuarios mencionados nesta obs
    mencoes = relationship(
        "ObservacaoMencao",
        back_populates="observacao",
        foreign_keys="ObservacaoMencao.observacao_id",
        lazy="selectin",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index(
            'idx_observacao_processo',
            'numero_processo',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_observacao_usuario',
            'usuario',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_observacao_equipe',
            'equipe_id',
            postgresql_where=text("deletado_em IS NULL AND equipe_id IS NOT NULL")
        ),
        Index(
            'idx_observacao_escopo',
            'numero_processo',
            'escopo',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_observacao_parent',
            'parent_id',
            postgresql_where=text("parent_id IS NOT NULL AND deletado_em IS NULL")
        ),
        {'comment': 'Tabela de observacoes sobre processos'}
    )

    def __repr__(self) -> str:
        return (
            f"<Observacao("
            f"id={self.id}, "
            f"numero_processo={self.numero_processo}, "
            f"usuario={self.usuario}"
            f")>"
        )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None
