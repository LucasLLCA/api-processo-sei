"""mencoes e respostas em observacoes

Revision ID: 009_mencoes_respostas
Revises: 007_add_escopo_observacoes
Create Date: 2026-03-04
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


# revision identifiers, used by Alembic.
revision: str = '009_mencoes_respostas'
down_revision: Union[str, None] = '007_add_escopo_observacoes'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Adiciona coluna parent_id em observacoes (auto-referência para respostas)
    op.add_column(
        'observacoes',
        sa.Column(
            'parent_id',
            UUID(as_uuid=True),
            sa.ForeignKey('observacoes.id', ondelete='SET NULL'),
            nullable=True,
            comment="ID da observacao pai (quando for uma resposta)",
        ),
    )

    op.create_index(
        'idx_observacao_parent',
        'observacoes',
        ['parent_id'],
        postgresql_where=sa.text("parent_id IS NOT NULL AND deletado_em IS NULL"),
    )

    # 2. Cria tabela observacao_mencoes
    op.create_table(
        'observacao_mencoes',
        sa.Column(
            'id',
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text('gen_random_uuid()'),
            comment="Identificador unico da mencao",
        ),
        sa.Column(
            'observacao_id',
            UUID(as_uuid=True),
            sa.ForeignKey('observacoes.id', ondelete='CASCADE'),
            nullable=False,
            comment="Observacao onde a mencao foi feita",
        ),
        sa.Column(
            'usuario_mencionado',
            sa.String(100),
            nullable=False,
            comment="Usuario mencionado",
        ),
        sa.Column(
            'visto_em',
            sa.TIMESTAMP(timezone=True),
            nullable=True,
            comment="Data e hora em que o usuario mencionado visualizou",
        ),
        sa.Column(
            'criado_em',
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text('CURRENT_TIMESTAMP'),
            comment="Data e hora da mencao",
        ),
        comment='Mencoes de usuarios em observacoes',
    )

    op.create_index(
        'idx_mencao_observacao',
        'observacao_mencoes',
        ['observacao_id'],
    )

    op.create_index(
        'idx_mencao_usuario_nao_visto',
        'observacao_mencoes',
        ['usuario_mencionado'],
        postgresql_where=sa.text("visto_em IS NULL"),
    )


def downgrade() -> None:
    op.drop_index('idx_mencao_usuario_nao_visto', table_name='observacao_mencoes')
    op.drop_index('idx_mencao_observacao', table_name='observacao_mencoes')
    op.drop_table('observacao_mencoes')
    op.drop_index('idx_observacao_parent', table_name='observacoes')
    op.drop_column('observacoes', 'parent_id')
