"""
Model SQLAlchemy para histórico de pesquisas de processos - PostgreSQL
"""
from sqlalchemy import Column, String, Text, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from ..database import Base


class HistoricoPesquisa(Base):
    """
    Model para armazenar histórico de pesquisas de processos

    Implementa soft delete através do campo deletado_em
    """
    __tablename__ = "historico_pesquisas"

    # Campos
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador único da pesquisa"
    )

    numero_processo = Column(
        String(50),
        nullable=False,
        comment="Número do processo sem formatação"
    )

    numero_processo_formatado = Column(
        String(50),
        nullable=True,
        comment="Número do processo formatado (ex: 12345.678901/2024-99)"
    )

    usuario = Column(
        String(100),
        nullable=False,
        comment="Identificação do usuário (email, CPF, username, etc)"
    )

    id_unidade = Column(
        String(50),
        nullable=True,
        comment="ID da unidade usada para acessar o processo"
    )

    caixa_contexto = Column(
        Text,
        nullable=True,
        comment="Campo de texto livre para contexto adicional"
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criação do registro"
    )

    atualizado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora da última atualização"
    )

    deletado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora da exclusão (soft delete)"
    )

    # Índices compostos para melhor performance no PostgreSQL
    __table_args__ = (
        Index(
            'idx_historico_usuario',
            'usuario',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_historico_numero_processo',
            'numero_processo',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_historico_criado_em',
            'criado_em',
            postgresql_using='btree',
            postgresql_ops={'criado_em': 'DESC'},
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_historico_usuario_criado',
            'usuario',
            'criado_em',
            postgresql_using='btree',
            postgresql_ops={'criado_em': 'DESC'},
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_historico_usuario_processo_unidade',
            'usuario',
            'numero_processo',
            'id_unidade',
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Tabela de histórico de pesquisas de processos do SEI'}
    )

    def __repr__(self) -> str:
        return (
            f"<HistoricoPesquisa("
            f"id={self.id}, "
            f"numero_processo={self.numero_processo}, "
            f"usuario={self.usuario}, "
            f"deletado={self.deletado_em is not None}"
            f")>"
        )

    def soft_delete(self) -> None:
        """Marca o registro como deletado (soft delete)"""
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        """Restaura um registro deletado"""
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        """Verifica se o registro está deletado"""
        return self.deletado_em is not None

    def to_dict(self) -> dict:
        """Converte o model para dicionário"""
        return {
            "id": str(self.id),
            "numero_processo": self.numero_processo,
            "numero_processo_formatado": self.numero_processo_formatado,
            "usuario": self.usuario,
            "id_unidade": self.id_unidade,
            "caixa_contexto": self.caixa_contexto,
            "criado_em": self.criado_em.isoformat() if self.criado_em else None,
            "atualizado_em": self.atualizado_em.isoformat() if self.atualizado_em else None,
            "deletado_em": self.deletado_em.isoformat() if self.deletado_em else None,
        }
