"""Create tipos_documento table

Revision ID: 014_tipos_documento
Revises: 013_unidades_sei
Create Date: 2026-03-23

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = "014_tipos_documento"
down_revision: Union[str, None] = "013_unidades_sei"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    if "tipos_documento" not in inspector.get_table_names():
        op.create_table(
            "tipos_documento",
            sa.Column("id_tipo_documento", sa.BigInteger(), nullable=False, comment="ID do tipo de documento no SEI"),
            sa.Column("nome", sa.String(500), nullable=False, comment="Nome do tipo de documento"),
            sa.Column(
                "criado_em",
                sa.TIMESTAMP(timezone=True),
                nullable=False,
                server_default=sa.text("CURRENT_TIMESTAMP"),
                comment="Data de criação",
            ),
            sa.PrimaryKeyConstraint("id_tipo_documento"),
            comment="Tipos de documento do SEI",
        )
        op.create_index("idx_tipo_documento_nome", "tipos_documento", ["nome"])


def downgrade() -> None:
    op.drop_index("idx_tipo_documento_nome", table_name="tipos_documento")
    op.drop_table("tipos_documento")
