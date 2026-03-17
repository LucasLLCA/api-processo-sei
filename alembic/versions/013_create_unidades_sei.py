"""Create unidades_sei table

Revision ID: 013_unidades_sei
Revises: a3d9a223862b
Create Date: 2026-03-17

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


# revision identifiers, used by Alembic.
revision: str = "013_unidades_sei"
down_revision: Union[str, None] = "54e03efa3b57"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = inspect(conn)
    if "unidades_sei" not in inspector.get_table_names():
        op.create_table(
            "unidades_sei",
            sa.Column("id_unidade", sa.Integer(), nullable=False, comment="ID da unidade no SEI"),
            sa.Column("sigla", sa.String(300), nullable=False, comment="Sigla/código da unidade"),
            sa.Column("descricao", sa.String(500), nullable=False, comment="Descrição completa da unidade"),
            sa.Column("criado_em", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP"), comment="Data de criação"),
            sa.PrimaryKeyConstraint("id_unidade"),
            comment="Unidades organizacionais do SEI",
        )
        op.create_index("idx_unidade_sei_sigla", "unidades_sei", ["sigla"])


def downgrade() -> None:
    op.drop_index("idx_unidade_sei_sigla", table_name="unidades_sei")
    op.drop_table("unidades_sei")
