"""
RBAC: definições de módulos e helpers de permissão.
"""
import logging
from typing import Callable

from fastapi import Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .database import get_db
from .models.credencial_usuario import CredencialUsuario
from .models.papel import Papel
from .models.usuario_papel import UsuarioPapel

logger = logging.getLogger(__name__)

# --------------- Authoritative module list ---------------

MODULOS: dict[str, str] = {
    "home": "Início",
    "processo_visualizar": "Visualizar Processo",
    "equipes": "Equipes",
    "bi": "Business Intelligence",
    "fluxos": "Fluxos de Processos",
    "admin": "Administração",
    "financeiro": "Dados Financeiros",
}


# --------------- Helpers ---------------

async def get_user_modulos(db: AsyncSession, usuario_sei: str) -> list[str]:
    """
    Returns the list of allowed module keys for a usuario_sei.
    Falls back to the default role if no assignment exists.
    """
    result = await db.execute(
        select(Papel.modulos)
        .join(UsuarioPapel, UsuarioPapel.papel_id == Papel.id)
        .where(
            UsuarioPapel.usuario_sei == usuario_sei,
            UsuarioPapel.deletado_em.is_(None),
            Papel.deletado_em.is_(None),
        )
    )
    row = result.scalar_one_or_none()
    if row is not None:
        return list(row)

    # Fallback: default role
    default_result = await db.execute(
        select(Papel.modulos).where(
            Papel.is_default.is_(True),
            Papel.deletado_em.is_(None),
        )
    )
    default_modulos = default_result.scalar_one_or_none()
    return list(default_modulos) if default_modulos else []


async def get_user_role_info(db: AsyncSession, usuario_sei: str) -> dict:
    """
    Returns full role info for a usuario_sei: {modulos, papel_nome, papel_slug}.
    Falls back to default role if no assignment exists.
    """
    result = await db.execute(
        select(Papel)
        .join(UsuarioPapel, UsuarioPapel.papel_id == Papel.id)
        .where(
            UsuarioPapel.usuario_sei == usuario_sei,
            UsuarioPapel.deletado_em.is_(None),
            Papel.deletado_em.is_(None),
        )
    )
    papel = result.scalar_one_or_none()

    if papel is None:
        # Fallback: default role
        default_result = await db.execute(
            select(Papel).where(
                Papel.is_default.is_(True),
                Papel.deletado_em.is_(None),
            )
        )
        papel = default_result.scalar_one_or_none()

    if papel is None:
        return {"modulos": [], "papel_nome": "Sem papel", "papel_slug": "none"}

    return {
        "modulos": list(papel.modulos),
        "papel_nome": papel.nome,
        "papel_slug": papel.slug,
    }


# --------------- FastAPI dependencies ---------------

def require_modulo(modulo_key: str) -> Callable:
    """
    FastAPI dependency factory. Returns a dependency that checks if the
    requesting user has access to the given module.
    """
    async def _dependency(
        id_pessoa: int = Query(..., alias="id_pessoa"),
        db: AsyncSession = Depends(get_db),
    ) -> CredencialUsuario:
        # Look up credential to get usuario_sei
        result = await db.execute(
            select(CredencialUsuario).where(
                CredencialUsuario.id_pessoa == id_pessoa,
                CredencialUsuario.deletado_em.is_(None),
            )
        )
        cred = result.scalar_one_or_none()
        if not cred:
            raise HTTPException(status_code=403, detail="Credenciais não encontradas")

        modulos = await get_user_modulos(db, cred.usuario_sei)
        if modulo_key not in modulos:
            raise HTTPException(
                status_code=403,
                detail=f"Acesso negado: módulo '{modulo_key}' não permitido",
            )
        return cred

    return _dependency
