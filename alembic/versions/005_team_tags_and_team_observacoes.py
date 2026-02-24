"""add team_tags, processo_team_tags tables and observacoes.equipe_id

Revision ID: 005_team_tags_obs_equipe
Revises: 004_add_observacoes
Create Date: 2026-02-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '005_team_tags_obs_equipe'
down_revision: Union[str, None] = '004_add_observacoes'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- team_tags ---
    op.create_table(
        'team_tags',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('equipe_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('equipes.id', ondelete='CASCADE'), nullable=False),
        sa.Column('nome', sa.String(100), nullable=False),
        sa.Column('cor', sa.String(7), nullable=True),
        sa.Column('criado_por', sa.String(100), nullable=False),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('atualizado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        comment='Tags de equipe para rotular processos no kanban'
    )

    op.create_index(
        'uq_team_tag_equipe_nome',
        'team_tags',
        ['equipe_id', 'nome'],
        unique=True,
        postgresql_where=sa.text('deletado_em IS NULL')
    )
    op.create_index(
        'idx_team_tag_equipe',
        'team_tags',
        ['equipe_id'],
        postgresql_where=sa.text('deletado_em IS NULL')
    )

    # --- processo_team_tags ---
    op.create_table(
        'processo_team_tags',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text('gen_random_uuid()'), nullable=False),
        sa.Column('team_tag_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('team_tags.id', ondelete='CASCADE'), nullable=False),
        sa.Column('numero_processo', sa.String(50), nullable=False),
        sa.Column('adicionado_por', sa.String(100), nullable=False),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        comment='Associacao entre processos e tags de equipe'
    )

    op.create_index(
        'uq_processo_team_tag',
        'processo_team_tags',
        ['team_tag_id', 'numero_processo'],
        unique=True,
        postgresql_where=sa.text('deletado_em IS NULL')
    )
    op.create_index(
        'idx_processo_team_tag_numero',
        'processo_team_tags',
        ['numero_processo'],
        postgresql_where=sa.text('deletado_em IS NULL')
    )

    # --- observacoes: add equipe_id ---
    op.add_column(
        'observacoes',
        sa.Column('equipe_id', postgresql.UUID(as_uuid=True), sa.ForeignKey('equipes.id', ondelete='SET NULL'), nullable=True),
    )
    op.create_index(
        'idx_observacao_equipe',
        'observacoes',
        ['equipe_id'],
        postgresql_where=sa.text('deletado_em IS NULL AND equipe_id IS NOT NULL')
    )


def downgrade() -> None:
    op.drop_index('idx_observacao_equipe', table_name='observacoes')
    op.drop_column('observacoes', 'equipe_id')

    op.drop_index('idx_processo_team_tag_numero', table_name='processo_team_tags')
    op.drop_index('uq_processo_team_tag', table_name='processo_team_tags')
    op.drop_table('processo_team_tags')

    op.drop_index('idx_team_tag_equipe', table_name='team_tags')
    op.drop_index('uq_team_tag_equipe_nome', table_name='team_tags')
    op.drop_table('team_tags')
