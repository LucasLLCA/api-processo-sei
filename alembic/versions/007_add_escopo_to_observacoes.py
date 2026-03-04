"""add escopo to observacoes

Revision ID: 007_add_escopo_observacoes
Revises: 006_processo_entendimento
Create Date: 2026-03-04
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '007_add_escopo_observacoes'
down_revision: Union[str, None] = '008_id_pessoa_bigint'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Adiciona coluna escopo com default 'global'
    op.add_column(
        'observacoes',
        sa.Column(
            'escopo',
            sa.String(10),
            nullable=False,
            server_default='global',
            comment="Escopo da observacao: pessoal | equipe | global",
        ),
    )

    # 2. Migra registros existentes:
    #    - equipe_id IS NOT NULL → escopo = 'equipe'
    #    - equipe_id IS NULL     → escopo = 'global' (já é o default)
    op.execute(
        """
        UPDATE observacoes
        SET escopo = 'equipe'
        WHERE equipe_id IS NOT NULL
          AND deletado_em IS NULL
        """
    )

    # 3. Índice para buscas por escopo + processo
    op.create_index(
        'idx_observacao_escopo',
        'observacoes',
        ['numero_processo', 'escopo'],
        postgresql_where=sa.text("deletado_em IS NULL"),
    )


def downgrade() -> None:
    op.drop_index('idx_observacao_escopo', table_name='observacoes')
    op.drop_column('observacoes', 'escopo')
