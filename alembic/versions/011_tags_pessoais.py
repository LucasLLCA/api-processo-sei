"""Make team_tags.equipe_id nullable for personal tags

Revision ID: 011_tags_pessoais
Revises: 010_add_cpf_credenciais
Create Date: 2026-03-13
"""
from alembic import op

revision = "011_tags_pessoais"
down_revision = "010_add_cpf_credenciais"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Make equipe_id nullable
    op.alter_column("team_tags", "equipe_id", nullable=True)

    # Drop old unique constraint (equipe_id, nome) and recreate scoped to non-null equipe_id
    op.drop_index("uq_team_tag_equipe_nome", table_name="team_tags")
    op.create_index(
        "uq_team_tag_equipe_nome",
        "team_tags",
        ["equipe_id", "nome"],
        unique=True,
        postgresql_where="deletado_em IS NULL AND equipe_id IS NOT NULL",
    )

    # Add unique constraint for personal tags (per user)
    op.create_index(
        "uq_team_tag_pessoal_nome",
        "team_tags",
        ["criado_por", "nome"],
        unique=True,
        postgresql_where="deletado_em IS NULL AND equipe_id IS NULL",
    )

    # Add index for personal tags lookup
    op.create_index(
        "idx_team_tag_pessoal",
        "team_tags",
        ["criado_por"],
        postgresql_where="deletado_em IS NULL AND equipe_id IS NULL",
    )


def downgrade() -> None:
    op.drop_index("idx_team_tag_pessoal", table_name="team_tags")
    op.drop_index("uq_team_tag_pessoal_nome", table_name="team_tags")

    op.drop_index("uq_team_tag_equipe_nome", table_name="team_tags")
    op.create_index(
        "uq_team_tag_equipe_nome",
        "team_tags",
        ["equipe_id", "nome"],
        unique=True,
        postgresql_where="deletado_em IS NULL",
    )

    op.alter_column("team_tags", "equipe_id", nullable=False)
