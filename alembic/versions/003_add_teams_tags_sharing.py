"""add teams, tags, and sharing tables

Revision ID: 003_teams_tags_sharing
Revises: 002_normalize_numero
Create Date: 2026-02-23
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = '003_teams_tags_sharing'
down_revision: Union[str, None] = '002_normalize_numero'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- equipes ---
    op.create_table(
        'equipes',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column('nome', sa.String(200), nullable=False),
        sa.Column('descricao', sa.Text(), nullable=True),
        sa.Column('proprietario_usuario', sa.String(100), nullable=False),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column('atualizado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        comment='Tabela de equipes de trabalho'
    )
    op.create_index(
        'idx_equipe_proprietario', 'equipes', ['proprietario_usuario'],
        postgresql_where=sa.text("deletado_em IS NULL")
    )

    # --- equipe_membros ---
    op.create_table(
        'equipe_membros',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column('equipe_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('usuario', sa.String(100), nullable=False),
        sa.Column('papel', sa.String(20), server_default=sa.text("'member'"), nullable=False),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['equipe_id'], ['equipes.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        comment='Tabela de membros de equipe'
    )
    op.create_index(
        'idx_membro_equipe', 'equipe_membros', ['equipe_id'],
        postgresql_where=sa.text("deletado_em IS NULL")
    )
    op.create_index(
        'idx_membro_usuario', 'equipe_membros', ['usuario'],
        postgresql_where=sa.text("deletado_em IS NULL")
    )
    # Partial unique index (acts as conditional unique constraint)
    op.create_index(
        'uq_equipe_membro_usuario', 'equipe_membros', ['equipe_id', 'usuario'],
        unique=True,
        postgresql_where=sa.text("deletado_em IS NULL")
    )

    # --- tags ---
    op.create_table(
        'tags',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column('nome', sa.String(200), nullable=False),
        sa.Column('usuario', sa.String(100), nullable=False),
        sa.Column('cor', sa.String(7), nullable=True),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column('atualizado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        comment='Tabela de tags para agrupamento de processos salvos'
    )
    op.create_index(
        'idx_tag_usuario', 'tags', ['usuario'],
        postgresql_where=sa.text("deletado_em IS NULL")
    )
    op.create_index(
        'uq_tag_usuario_nome', 'tags', ['usuario', 'nome'],
        unique=True,
        postgresql_where=sa.text("deletado_em IS NULL")
    )

    # --- processos_salvos ---
    op.create_table(
        'processos_salvos',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column('tag_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('numero_processo', sa.String(50), nullable=False),
        sa.Column('numero_processo_formatado', sa.String(50), nullable=True),
        sa.Column('nota', sa.Text(), nullable=True),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['tag_id'], ['tags.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        comment='Tabela de processos salvos em tags'
    )
    op.create_index(
        'idx_processo_salvo_tag', 'processos_salvos', ['tag_id'],
        postgresql_where=sa.text("deletado_em IS NULL")
    )
    op.create_index(
        'idx_processo_salvo_numero', 'processos_salvos', ['numero_processo'],
        postgresql_where=sa.text("deletado_em IS NULL")
    )
    op.create_index(
        'uq_processo_salvo_tag_numero', 'processos_salvos', ['tag_id', 'numero_processo'],
        unique=True,
        postgresql_where=sa.text("deletado_em IS NULL")
    )

    # --- compartilhamentos ---
    op.create_table(
        'compartilhamentos',
        sa.Column('id', postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column('tag_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('compartilhado_por', sa.String(100), nullable=False),
        sa.Column('equipe_destino_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('usuario_destino', sa.String(100), nullable=True),
        sa.Column('criado_em', sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column('deletado_em', sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['tag_id'], ['tags.id'], ondelete='CASCADE'),
        sa.ForeignKeyConstraint(['equipe_destino_id'], ['equipes.id'], ondelete='CASCADE'),
        sa.CheckConstraint(
            "(equipe_destino_id IS NOT NULL AND usuario_destino IS NULL) OR "
            "(equipe_destino_id IS NULL AND usuario_destino IS NOT NULL)",
            name="ck_compartilhamento_destino_exclusivo"
        ),
        sa.PrimaryKeyConstraint('id'),
        comment='Tabela de compartilhamentos de tags'
    )
    op.create_index(
        'idx_compartilhamento_tag', 'compartilhamentos', ['tag_id'],
        postgresql_where=sa.text("deletado_em IS NULL")
    )
    op.create_index(
        'idx_compartilhamento_usuario_destino', 'compartilhamentos', ['usuario_destino'],
        postgresql_where=sa.text("deletado_em IS NULL")
    )
    op.create_index(
        'idx_compartilhamento_equipe_destino', 'compartilhamentos', ['equipe_destino_id'],
        postgresql_where=sa.text("deletado_em IS NULL")
    )


def downgrade() -> None:
    op.drop_table('compartilhamentos')
    op.drop_table('processos_salvos')
    op.drop_table('tags')
    op.drop_table('equipe_membros')
    op.drop_table('equipes')
