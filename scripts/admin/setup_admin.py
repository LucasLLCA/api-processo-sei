#!/usr/bin/env python3
"""Grant the admin role to a user identified by ``id_pessoa``.

Outside the data pipeline — this is API-side admin bootstrap. Requires the
admin Papel to already exist (run ``alembic upgrade head`` first).

Usage:
    python scripts/admin/setup_admin.py <id_pessoa>
    python scripts/admin/setup_admin.py 10148

Reads PostgreSQL credentials from ``api.config.settings`` (DATABASE_*).
"""

import argparse
import asyncio
import sys
from pathlib import Path

_HERE = Path(__file__).resolve()
_SCRIPTS = next(p for p in _HERE.parents if p.name == "scripts")
_PROJECT = _SCRIPTS.parent
for _p in (_SCRIPTS, _PROJECT):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession  # noqa: E402

from api.config import settings  # noqa: E402
from api.models.credencial_usuario import CredencialUsuario  # noqa: E402
from api.models.papel import Papel  # noqa: E402
from api.models.usuario_papel import UsuarioPapel  # noqa: E402
from pipeline.logging_setup import configure_logging  # noqa: E402

log = configure_logging(__name__)


async def setup_admin(id_pessoa: int) -> None:
    db_url = (
        f"postgresql+asyncpg://{settings.DATABASE_USER}:{settings.DATABASE_PASSWORD}"
        f"@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{settings.DATABASE_NAME}"
    )
    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_factory() as db:
        result = await db.execute(
            select(CredencialUsuario).where(
                CredencialUsuario.id_pessoa == id_pessoa,
                CredencialUsuario.deletado_em.is_(None),
            )
        )
        cred = result.scalar_one_or_none()
        if not cred:
            log.error("No active credential found for id_pessoa=%s", id_pessoa)
            await engine.dispose()
            sys.exit(1)

        log.info("Found user: usuario_sei=%s, orgao=%s", cred.usuario_sei, cred.orgao)

        result = await db.execute(
            select(Papel).where(Papel.slug == "admin", Papel.deletado_em.is_(None))
        )
        admin_papel = result.scalar_one_or_none()
        if not admin_papel:
            log.error("Admin role not found. Run 'alembic upgrade head' first.")
            await engine.dispose()
            sys.exit(1)

        log.info("Admin role: id=%s, modulos=%s", admin_papel.id, admin_papel.modulos)

        result = await db.execute(
            select(UsuarioPapel).where(
                UsuarioPapel.usuario_sei == cred.usuario_sei,
                UsuarioPapel.deletado_em.is_(None),
            )
        )
        existing = result.scalar_one_or_none()

        if existing:
            if existing.papel_id == admin_papel.id:
                log.info("User %s is already admin. Nothing to do.", cred.usuario_sei)
                await engine.dispose()
                return
            existing.soft_delete()
            await db.flush()
            log.info("Soft-deleted previous role assignment (papel_id=%s)", existing.papel_id)

        assignment = UsuarioPapel(
            usuario_sei=cred.usuario_sei,
            papel_id=admin_papel.id,
            atribuido_por="setup_admin_script",
        )
        db.add(assignment)
        await db.commit()

        log.info("SUCCESS: %s (id_pessoa=%s) is now admin", cred.usuario_sei, id_pessoa)
        log.info("  Modulos: %s", admin_papel.modulos)

    await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Grant admin role to a user by id_pessoa")
    parser.add_argument("id_pessoa", type=int, help="Numeric id_pessoa of the target user")
    args = parser.parse_args()
    asyncio.run(setup_admin(args.id_pessoa))


if __name__ == "__main__":
    main()
