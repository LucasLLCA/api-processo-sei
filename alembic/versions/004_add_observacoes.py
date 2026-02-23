"""add observacoes table

Revision ID: 004_add_observacoes
Revises: 003_teams_tags_sharing
Create Date: 2026-02-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '004_add_observacoes'
down_revision: Union[str, None] = '003_teams_tags_sharing'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'observacoes',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('numero_processo', sa.String(50), nullable=False),
        sa.Column('usuario', sa.String(100), nullable=False),
        sa.Column('conteudo', sa.Text(), nullable=False),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('atualizado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        comment='Tabela de observacoes sobre processos'
    )

    op.create_index(
        'idx_observacao_processo',
        'observacoes',
        ['numero_processo'],
        postgresql_where=sa.text('deletado_em IS NULL')
    )
    op.create_index(
        'idx_observacao_usuario',
        'observacoes',
        ['usuario'],
        postgresql_where=sa.text('deletado_em IS NULL')
    )


def downgrade() -> None:
    op.drop_index('idx_observacao_usuario', table_name='observacoes')
    op.drop_index('idx_observacao_processo', table_name='observacoes')
    op.drop_table('observacoes')
