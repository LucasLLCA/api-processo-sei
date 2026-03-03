"""change id_pessoa from integer to bigint

Revision ID: 008_id_pessoa_bigint
Revises: 007_add_credenciais_usuario
Create Date: 2026-03-03
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '008_id_pessoa_bigint'
down_revision: Union[str, None] = '007_add_credenciais_usuario'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        'credenciais_usuario',
        'id_pessoa',
        type_=sa.BigInteger(),
        existing_type=sa.Integer(),
        existing_nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        'credenciais_usuario',
        'id_pessoa',
        type_=sa.Integer(),
        existing_type=sa.BigInteger(),
        existing_nullable=False,
    )
