"""
Model SQLAlchemy para registro de atividades do sistema - PostgreSQL
"""
from sqlalchemy import Column, String, Text, Integer, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime
import uuid

from ..database import Base


class RegistroAtividade(Base):
    """
    Model para armazenar log de atividades dos usuarios no sistema.
    Implementa soft delete atraves do campo deletado_em.
    """
    __tablename__ = "registro_atividades"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
        comment="Identificador unico da atividade"
    )

    usuario_sei = Column(
        String(100),
        nullable=False,
        comment="Identificacao do usuario (email SEI)"
    )

    tipo_atividade = Column(
        String(50),
        nullable=False,
        comment="Tipo de atividade: login, visualizar_processo, gerar_resumo, etc."
    )

    recurso = Column(
        String(255),
        nullable=True,
        comment="Recurso envolvido (numero do processo, ID do fluxo, etc.)"
    )

    detalhes = Column(
        Text,
        nullable=True,
        comment="Detalhes adicionais em formato texto/JSON"
    )

    ip_address = Column(
        String(45),
        nullable=True,
        comment="Endereco IP do cliente"
    )

    rota = Column(
        String(255),
        nullable=True,
        comment="Rota da API acessada"
    )

    metodo_http = Column(
        String(10),
        nullable=True,
        comment="Metodo HTTP (GET, POST, etc.)"
    )

    status_code = Column(
        Integer,
        nullable=True,
        comment="Codigo de status HTTP da resposta"
    )

    duracao_ms = Column(
        Integer,
        nullable=True,
        comment="Duracao da requisicao em milissegundos"
    )

    orgao = Column(
        String(50),
        nullable=True,
        comment="Orgao do usuario (denormalizado para performance)"
    )

    criado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        comment="Data e hora de criacao do registro"
    )

    deletado_em = Column(
        TIMESTAMP(timezone=True),
        nullable=True,
        comment="Data e hora da exclusao (soft delete)"
    )

    __table_args__ = (
        Index(
            'idx_atividade_usuario_sei',
            'usuario_sei',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_atividade_tipo',
            'tipo_atividade',
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_atividade_criado_em',
            'criado_em',
            postgresql_using='btree',
            postgresql_ops={'criado_em': 'DESC'},
            postgresql_where=text("deletado_em IS NULL")
        ),
        Index(
            'idx_atividade_usuario_criado',
            'usuario_sei',
            'criado_em',
            postgresql_using='btree',
            postgresql_ops={'criado_em': 'DESC'},
            postgresql_where=text("deletado_em IS NULL")
        ),
        {'comment': 'Log de atividades dos usuarios no sistema'}
    )

    def __repr__(self) -> str:
        return (
            f"<RegistroAtividade("
            f"id={self.id}, "
            f"usuario_sei={self.usuario_sei}, "
            f"tipo_atividade={self.tipo_atividade}"
            f")>"
        )

    def soft_delete(self) -> None:
        self.deletado_em = datetime.utcnow()

    def restore(self) -> None:
        self.deletado_em = None

    @property
    def is_deleted(self) -> bool:
        return self.deletado_em is not None

    def to_dict(self) -> dict:
        return {
            "id": str(self.id),
            "usuario_sei": self.usuario_sei,
            "tipo_atividade": self.tipo_atividade,
            "recurso": self.recurso,
            "detalhes": self.detalhes,
            "ip_address": self.ip_address,
            "rota": self.rota,
            "metodo_http": self.metodo_http,
            "status_code": self.status_code,
            "duracao_ms": self.duracao_ms,
            "orgao": self.orgao,
            "criado_em": self.criado_em.isoformat() if self.criado_em else None,
            "deletado_em": self.deletado_em.isoformat() if self.deletado_em else None,
        }
