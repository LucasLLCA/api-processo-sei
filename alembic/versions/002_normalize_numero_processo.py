"""normalize numero_processo to digits-only

Revision ID: 002_normalize_numero
Revises: 001_add_id_unidade
Create Date: 2026-02-13
"""
from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = '002_normalize_numero'
down_revision: Union[str, None] = '001_add_id_unidade'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Strip all non-digit characters from numero_processo
    op.execute(
        "UPDATE historico_pesquisas "
        "SET numero_processo = regexp_replace(numero_processo, '[^0-9]', '', 'g') "
        "WHERE numero_processo ~ '[^0-9]'"
    )

    # Backfill numero_processo_formatado where NULL and numero_processo is 17 digits
    op.execute(
        "UPDATE historico_pesquisas "
        "SET numero_processo_formatado = "
        "  substring(numero_processo from 1 for 5) || '.' || "
        "  substring(numero_processo from 6 for 6) || '/' || "
        "  substring(numero_processo from 12 for 4) || '-' || "
        "  substring(numero_processo from 16 for 2) "
        "WHERE numero_processo_formatado IS NULL "
        "  AND numero_processo ~ '^[0-9]{17}$'"
    )


def downgrade() -> None:
    # Data migration — no structural rollback needed
    pass
