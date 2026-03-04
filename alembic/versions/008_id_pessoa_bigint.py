"""id pessoa bigint

Revision ID: 008_id_pessoa_bigint
Revises: 006_processo_entendimento
Create Date: 2026-03-04

NOTE: Este arquivo é um stub recriado porque o original foi perdido.
      A migration já foi aplicada ao banco — upgrade/downgrade são no-ops.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '008_id_pessoa_bigint'
down_revision: Union[str, None] = '006_processo_entendimento'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Stub: migration já aplicada ao banco.
    pass


def downgrade() -> None:
    # Stub: migration já aplicada ao banco.
    pass
