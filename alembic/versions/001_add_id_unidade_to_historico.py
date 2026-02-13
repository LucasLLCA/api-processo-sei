"""add id_unidade to historico_pesquisas

Revision ID: 001_add_id_unidade
Revises:
Create Date: 2026-02-12
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '001_add_id_unidade'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add id_unidade column (nullable so old rows keep NULL)
    op.add_column(
        'historico_pesquisas',
        sa.Column('id_unidade', sa.String(50), nullable=True, comment='ID da unidade usada para acessar o processo')
    )

    # Add composite index for dedup on (usuario, numero_processo, id_unidade)
    op.create_index(
        'idx_historico_usuario_processo_unidade',
        'historico_pesquisas',
        ['usuario', 'numero_processo', 'id_unidade'],
        postgresql_where=sa.text('deletado_em IS NULL')
    )


def downgrade() -> None:
    op.drop_index('idx_historico_usuario_processo_unidade', table_name='historico_pesquisas')
    op.drop_column('historico_pesquisas', 'id_unidade')
