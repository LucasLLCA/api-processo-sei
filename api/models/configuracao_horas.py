"""
Model SQLAlchemy para configuração de horas por tipo de andamento por órgão.
"""
from sqlalchemy import Column, String, Float, TIMESTAMP, Index, text
from sqlalchemy.dialects.postgresql import UUID
import uuid

from ..database import Base


class ConfiguracaoHorasAndamento(Base):
    """
    Coeficientes de horas por tarefa de andamento, configuráveis por órgão.
    Cada linha mapeia um grupo de tarefas (grupo_key) a um número de horas
    por ocorrência, permitindo converter contagem de produtividade em horas.
    """
    __tablename__ = "configuracao_horas_andamento"

    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        server_default=text("gen_random_uuid()"),
    )
    orgao = Column(String(100), nullable=False)
    grupo_key = Column(String(50), nullable=False)
    horas = Column(Float, nullable=False, default=0.0)
    atualizado_em = Column(
        TIMESTAMP(timezone=True),
        server_default=text("CURRENT_TIMESTAMP"),
    )
    atualizado_por = Column(String(100), nullable=True)

    __table_args__ = (
        Index('uq_orgao_grupo', 'orgao', 'grupo_key', unique=True),
    )
