"""add credenciais_usuario table

Revision ID: 007_add_credenciais_usuario
Revises: 006_processo_entendimento
Create Date: 2026-02-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '007_add_credenciais_usuario'
down_revision: Union[str, None] = '006_processo_entendimento'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'credenciais_usuario',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('id_pessoa', sa.Integer(), nullable=False),
        sa.Column('usuario_sei', sa.String(100), nullable=False),
        sa.Column('senha_encrypted', sa.Text(), nullable=False),
        sa.Column('orgao', sa.String(50), nullable=False),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('atualizado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        comment='Credenciais SEI criptografadas por usuário'
    )

    op.create_index(
        'idx_credencial_id_pessoa_unique',
        'credenciais_usuario',
        ['id_pessoa'],
        unique=True,
        postgresql_where=sa.text('deletado_em IS NULL'),
    )


def downgrade() -> None:
    op.drop_index('idx_credencial_id_pessoa_unique', table_name='credenciais_usuario')
    op.drop_table('credenciais_usuario')
