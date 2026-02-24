"""add processo_entendimentos table

Revision ID: 006_processo_entendimento
Revises: 005_team_tags_obs_equipe
Create Date: 2026-02-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '006_processo_entendimento'
down_revision: Union[str, None] = '005_team_tags_obs_equipe'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'processo_entendimentos',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('numero_processo', sa.String(50), nullable=False),
        sa.Column('conteudo', sa.Text(), nullable=False),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('atualizado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        comment='Entendimentos de processos gerados por IA'
    )

    op.create_index(
        'idx_entendimento_processo_unique',
        'processo_entendimentos',
        ['numero_processo'],
        unique=True,
        postgresql_where=sa.text('deletado_em IS NULL'),
    )


def downgrade() -> None:
    op.drop_index('idx_entendimento_processo_unique', table_name='processo_entendimentos')
    op.drop_table('processo_entendimentos')
