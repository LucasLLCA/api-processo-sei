#!/usr/bin/env python3
"""
Script to set up admin role for a user by id_pessoa.

Usage:
    python scripts/setup_admin.py <id_pessoa>
    python scripts/setup_admin.py 10148

Requires DATABASE_URL env vars or .env file in api-processo-sei/.
"""
import asyncio
import sys
import os
from pathlib import Path

# Add parent to path so we can import the app modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

from api.models.credencial_usuario import CredencialUsuario
from api.models.papel import Papel
from api.models.usuario_papel import UsuarioPapel
from api.config import settings


async def setup_admin(id_pessoa: int):
    db_url = (
        f"postgresql+asyncpg://{settings.DATABASE_USER}:{settings.DATABASE_PASSWORD}"
        f"@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{settings.DATABASE_NAME}"
    )
    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as db:
        # 1. Find the user's credential
        result = await db.execute(
            select(CredencialUsuario).where(
                CredencialUsuario.id_pessoa == id_pessoa,
                CredencialUsuario.deletado_em.is_(None),
            )
        )
        cred = result.scalar_one_or_none()
        if not cred:
            print(f"ERROR: No active credential found for id_pessoa={id_pessoa}")
            await engine.dispose()
            sys.exit(1)

        print(f"Found user: usuario_sei={cred.usuario_sei}, orgao={cred.orgao}")

        # 2. Find the admin role
        result = await db.execute(
            select(Papel).where(Papel.slug == "admin", Papel.deletado_em.is_(None))
        )
        admin_papel = result.scalar_one_or_none()
        if not admin_papel:
            print("ERROR: Admin role not found. Run 'alembic upgrade head' first.")
            await engine.dispose()
            sys.exit(1)

        print(f"Admin role: id={admin_papel.id}, modulos={admin_papel.modulos}")

        # 3. Check existing assignment
        result = await db.execute(
            select(UsuarioPapel).where(
                UsuarioPapel.usuario_sei == cred.usuario_sei,
                UsuarioPapel.deletado_em.is_(None),
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            if existing.papel_id == admin_papel.id:
                print(f"User {cred.usuario_sei} is already admin. Nothing to do.")
                await engine.dispose()
                return

            # Soft-delete old assignment
            existing.soft_delete()
            await db.flush()
            print(f"Soft-deleted previous role assignment (papel_id={existing.papel_id})")

        # 4. Create new admin assignment
        assignment = UsuarioPapel(
            usuario_sei=cred.usuario_sei,
            papel_id=admin_papel.id,
            atribuido_por="setup_admin_script",
        )
        db.add(assignment)
        await db.commit()

        print(f"SUCCESS: {cred.usuario_sei} (id_pessoa={id_pessoa}) is now admin")
        print(f"  Modulos: {admin_papel.modulos}")

    await engine.dispose()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/setup_admin.py <id_pessoa>")
        sys.exit(1)

    try:
        id_pessoa = int(sys.argv[1])
    except ValueError:
        print(f"ERROR: id_pessoa must be an integer, got '{sys.argv[1]}'")
        sys.exit(1)

    asyncio.run(setup_admin(id_pessoa))
