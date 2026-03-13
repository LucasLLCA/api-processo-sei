"""Add equipe_id to tags table, soft-delete all compartilhamentos.

Groups (tags table) are now either personal (equipe_id IS NULL) or
team-owned (equipe_id IS NOT NULL). Sharing is no longer needed.

Revision ID: 012_grupos_equipe
Revises: 011_tags_pessoais
Create Date: 2026-03-13
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "012_grupos_equipe"
down_revision = "011_tags_pessoais"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # 1. Add equipe_id to tags table
    op.add_column(
        "tags",
        sa.Column("equipe_id", UUID(as_uuid=True), sa.ForeignKey("equipes.id", ondelete="CASCADE"), nullable=True),
    )

    # 2. Add indexes for team-owned grupos
    op.create_index(
        "idx_tags_equipe_id",
        "tags",
        ["equipe_id"],
        postgresql_where="deletado_em IS NULL AND equipe_id IS NOT NULL",
    )
    op.create_index(
        "uq_tags_equipe_nome",
        "tags",
        ["equipe_id", "nome"],
        unique=True,
        postgresql_where="deletado_em IS NULL AND equipe_id IS NOT NULL",
    )

    # 3. Soft-delete all compartilhamentos (sharing is removed)
    op.execute("UPDATE compartilhamentos SET deletado_em = NOW() WHERE deletado_em IS NULL")


def downgrade() -> None:
    # Restore compartilhamentos
    op.execute("UPDATE compartilhamentos SET deletado_em = NULL WHERE deletado_em IS NOT NULL")

    op.drop_index("uq_tags_equipe_nome", table_name="tags")
    op.drop_index("idx_tags_equipe_id", table_name="tags")
    op.drop_column("tags", "equipe_id")
