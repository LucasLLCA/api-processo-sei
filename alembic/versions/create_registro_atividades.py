"""Create registro_atividades table for activity logging

Revision ID: b7c8d9e0f1a2
Revises: a1b2c3d4e5f6
Create Date: 2026-03-30

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "b7c8d9e0f1a2"
down_revision: Union[str, None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    inspector = sa.inspect(conn)
    if "registro_atividades" in inspector.get_table_names():
        return

    op.create_table(
        "registro_atividades",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("usuario_sei", sa.String(100), nullable=False),
        sa.Column("tipo_atividade", sa.String(50), nullable=False),
        sa.Column("recurso", sa.String(255), nullable=True),
        sa.Column("detalhes", sa.Text(), nullable=True),
        sa.Column("ip_address", sa.String(45), nullable=True),
        sa.Column("rota", sa.String(255), nullable=True),
        sa.Column("metodo_http", sa.String(10), nullable=True),
        sa.Column("status_code", sa.Integer(), nullable=True),
        sa.Column("duracao_ms", sa.Integer(), nullable=True),
        sa.Column("orgao", sa.String(50), nullable=True),
        sa.Column("criado_em", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("deletado_em", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        comment="Log de atividades dos usuarios no sistema",
    )

    op.create_index(
        "idx_atividade_usuario_sei",
        "registro_atividades",
        ["usuario_sei"],
        postgresql_where=sa.text("deletado_em IS NULL"),
    )
    op.create_index(
        "idx_atividade_tipo",
        "registro_atividades",
        ["tipo_atividade"],
        postgresql_where=sa.text("deletado_em IS NULL"),
    )
    op.create_index(
        "idx_atividade_criado_em",
        "registro_atividades",
        [sa.text("criado_em DESC")],
        postgresql_where=sa.text("deletado_em IS NULL"),
    )
    op.create_index(
        "idx_atividade_usuario_criado",
        "registro_atividades",
        ["usuario_sei", sa.text("criado_em DESC")],
        postgresql_where=sa.text("deletado_em IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_atividade_usuario_criado", table_name="registro_atividades")
    op.drop_index("idx_atividade_criado_em", table_name="registro_atividades")
    op.drop_index("idx_atividade_tipo", table_name="registro_atividades")
    op.drop_index("idx_atividade_usuario_sei", table_name="registro_atividades")
    op.drop_table("registro_atividades")
