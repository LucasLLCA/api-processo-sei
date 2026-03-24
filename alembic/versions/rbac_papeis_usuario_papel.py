"""RBAC: create papeis and usuario_papel tables with seed data

Revision ID: a1b2c3d4e5f6
Revises: ecdb42ce6390
Create Date: 2026-03-24 10:30:00.000000

"""
from typing import Sequence, Union
import uuid

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "9b9b373ff052"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- 1. Create papeis table ---
    op.create_table(
        "papeis",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("nome", sa.String(60), nullable=False),
        sa.Column("slug", sa.String(40), nullable=False),
        sa.Column("descricao", sa.Text(), nullable=True),
        sa.Column("modulos", postgresql.ARRAY(sa.Text()), server_default=sa.text("'{}'"), nullable=False),
        sa.Column("is_default", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("criado_em", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("atualizado_em", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("deletado_em", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        comment="Papéis (roles) do sistema com módulos permitidos",
    )
    op.create_index(
        "idx_papel_slug_unique",
        "papeis",
        ["slug"],
        unique=True,
        postgresql_where=sa.text("deletado_em IS NULL"),
    )

    # --- 2. Create usuario_papel table ---
    op.create_table(
        "usuario_papel",
        sa.Column("id", postgresql.UUID(as_uuid=True), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("usuario_sei", sa.String(100), nullable=False),
        sa.Column("papel_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("atribuido_por", sa.String(100), nullable=True),
        sa.Column("criado_em", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("atualizado_em", sa.TIMESTAMP(timezone=True), server_default=sa.text("CURRENT_TIMESTAMP"), nullable=False),
        sa.Column("deletado_em", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["papel_id"], ["papeis.id"], ondelete="RESTRICT"),
        comment="Atribuição de papel por usuario_sei (compartilhado entre id_pessoa)",
    )
    op.create_index(
        "idx_usuario_papel_usuario_unique",
        "usuario_papel",
        ["usuario_sei"],
        unique=True,
        postgresql_where=sa.text("deletado_em IS NULL"),
    )
    op.create_index(
        "idx_usuario_papel_papel_id",
        "usuario_papel",
        ["papel_id"],
        postgresql_where=sa.text("deletado_em IS NULL"),
    )

    # --- 3. Seed default roles ---
    all_modulos = ["home", "processo_visualizar", "equipes", "bi", "fluxos", "admin", "financeiro"]
    beta_modulos = ["home", "processo_visualizar", "equipes", "bi", "fluxos", "financeiro"]
    user_modulos = ["home", "processo_visualizar", "equipes", "bi"]

    admin_id = str(uuid.uuid4())
    beta_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())

    papeis_table = sa.table(
        "papeis",
        sa.column("id", postgresql.UUID),
        sa.column("nome", sa.String),
        sa.column("slug", sa.String),
        sa.column("descricao", sa.Text),
        sa.column("modulos", postgresql.ARRAY(sa.Text)),
        sa.column("is_default", sa.Boolean),
    )

    op.bulk_insert(papeis_table, [
        {
            "id": admin_id,
            "nome": "Administrador",
            "slug": "admin",
            "descricao": "Acesso completo a todos os módulos do sistema",
            "modulos": all_modulos,
            "is_default": False,
        },
        {
            "id": beta_id,
            "nome": "Beta",
            "slug": "beta",
            "descricao": "Acesso estendido incluindo funcionalidades em teste",
            "modulos": beta_modulos,
            "is_default": False,
        },
        {
            "id": user_id,
            "nome": "Usuário",
            "slug": "user",
            "descricao": "Acesso padrão ao sistema",
            "modulos": user_modulos,
            "is_default": True,
        },
    ])

    # --- 4. Migrate existing papel_global data ---
    # For each distinct usuario_sei with active credentials,
    # create a usuario_papel row mapping to the matching role.
    # If multiple id_pessoa share the same usuario_sei with different roles,
    # pick the highest-privilege one (admin > beta > user).
    conn = op.get_bind()

    # Map slug -> seeded papel id
    slug_to_id = {"admin": admin_id, "beta": beta_id, "user": user_id}
    privilege_order = {"admin": 3, "beta": 2, "user": 1}

    rows = conn.execute(
        sa.text(
            "SELECT DISTINCT usuario_sei, papel_global "
            "FROM credenciais_usuario "
            "WHERE deletado_em IS NULL "
            "ORDER BY usuario_sei"
        )
    ).fetchall()

    # Group by usuario_sei, pick highest privilege
    user_roles: dict[str, str] = {}
    for usuario_sei, papel_global in rows:
        current = user_roles.get(usuario_sei)
        if current is None or privilege_order.get(papel_global, 0) > privilege_order.get(current, 0):
            user_roles[usuario_sei] = papel_global

    if user_roles:
        usuario_papel_table = sa.table(
            "usuario_papel",
            sa.column("id", postgresql.UUID),
            sa.column("usuario_sei", sa.String),
            sa.column("papel_id", postgresql.UUID),
            sa.column("atribuido_por", sa.String),
        )
        op.bulk_insert(usuario_papel_table, [
            {
                "id": str(uuid.uuid4()),
                "usuario_sei": usuario_sei,
                "papel_id": slug_to_id.get(papel, user_id),
                "atribuido_por": "migration",
            }
            for usuario_sei, papel in user_roles.items()
        ])

    # --- 5. Drop papel_global column (no longer used) ---
    op.drop_column("credenciais_usuario", "papel_global")


def downgrade() -> None:
    # Restore papel_global column
    op.add_column(
        "credenciais_usuario",
        sa.Column("papel_global", sa.String(20), server_default=sa.text("'user'"), nullable=False),
    )
    op.drop_index("idx_usuario_papel_papel_id", table_name="usuario_papel")
    op.drop_index("idx_usuario_papel_usuario_unique", table_name="usuario_papel")
    op.drop_table("usuario_papel")
    op.drop_index("idx_papel_slug_unique", table_name="papeis")
    op.drop_table("papeis")
