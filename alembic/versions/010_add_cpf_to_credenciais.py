"""add cpf column to credenciais_usuario

Revision ID: 010_add_cpf_credenciais
Revises: 009_mencoes_respostas
Create Date: 2026-03-05
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '010_add_cpf_credenciais'
down_revision: Union[str, None] = '009_mencoes_respostas'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'credenciais_usuario',
        sa.Column('cpf', sa.String(20), nullable=True),
    )


def downgrade() -> None:
    op.drop_column('credenciais_usuario', 'cpf')
