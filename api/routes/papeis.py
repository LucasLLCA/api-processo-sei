"""
Endpoints CRUD para papéis (roles) e atribuição de papéis a usuários.
Montado sob o prefixo /admin pelo routes/__init__.py.
"""
import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..rbac import MODULOS, require_modulo
from ..models.papel import Papel
from ..models.usuario_papel import UsuarioPapel

logger = logging.getLogger(__name__)

router = APIRouter()


# --------------- Schemas ---------------

class PapelCreate(BaseModel):
    nome: str
    slug: str
    descricao: Optional[str] = None
    modulos: List[str]


class PapelUpdate(BaseModel):
    nome: Optional[str] = None
    descricao: Optional[str] = None
    modulos: Optional[List[str]] = None


class UsuarioPapelAssign(BaseModel):
    usuario_sei: str
    papel_id: str


# --------------- Papeis CRUD ---------------

@router.get("/papeis")
async def listar_papeis(
    _admin=Depends(require_modulo("admin")),
    db: AsyncSession = Depends(get_db),
):
    """List all active roles."""
    result = await db.execute(
        select(Papel)
        .where(Papel.deletado_em.is_(None))
        .order_by(Papel.nome)
    )
    rows = result.scalars().all()
    return [
        {
            "id": str(r.id),
            "nome": r.nome,
            "slug": r.slug,
            "descricao": r.descricao,
            "modulos": list(r.modulos),
            "is_default": r.is_default,
            "criado_em": r.criado_em.isoformat() if r.criado_em else None,
            "atualizado_em": r.atualizado_em.isoformat() if r.atualizado_em else None,
        }
        for r in rows
    ]


@router.post("/papeis")
async def criar_papel(
    body: PapelCreate,
    _admin=Depends(require_modulo("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Create a new role. Validates modulos against the authoritative list."""
    invalid = [m for m in body.modulos if m not in MODULOS]
    if invalid:
        raise HTTPException(
            status_code=400,
            detail=f"Módulos inválidos: {', '.join(invalid)}. Válidos: {', '.join(MODULOS.keys())}",
        )

    # Check slug uniqueness
    existing = await db.execute(
        select(Papel).where(Papel.slug == body.slug, Papel.deletado_em.is_(None))
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail=f"Já existe um papel com slug '{body.slug}'")

    papel = Papel(
        nome=body.nome,
        slug=body.slug,
        descricao=body.descricao,
        modulos=body.modulos,
    )
    db.add(papel)
    await db.flush()

    return {
        "id": str(papel.id),
        "nome": papel.nome,
        "slug": papel.slug,
        "descricao": papel.descricao,
        "modulos": list(papel.modulos),
        "is_default": papel.is_default,
    }


@router.patch("/papeis/{papel_id}")
async def atualizar_papel(
    body: PapelUpdate,
    papel_id: str = Path(...),
    _admin=Depends(require_modulo("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Update a role's name, description, or modulos."""
    result = await db.execute(
        select(Papel).where(Papel.id == papel_id, Papel.deletado_em.is_(None))
    )
    papel = result.scalar_one_or_none()
    if not papel:
        raise HTTPException(status_code=404, detail="Papel não encontrado")

    if body.modulos is not None:
        invalid = [m for m in body.modulos if m not in MODULOS]
        if invalid:
            raise HTTPException(
                status_code=400,
                detail=f"Módulos inválidos: {', '.join(invalid)}",
            )
        papel.modulos = body.modulos

    if body.nome is not None:
        papel.nome = body.nome
    if body.descricao is not None:
        papel.descricao = body.descricao

    papel.atualizado_em = datetime.now(timezone.utc)
    await db.flush()

    return {
        "id": str(papel.id),
        "nome": papel.nome,
        "slug": papel.slug,
        "descricao": papel.descricao,
        "modulos": list(papel.modulos),
        "is_default": papel.is_default,
    }


@router.delete("/papeis/{papel_id}")
async def deletar_papel(
    papel_id: str = Path(...),
    _admin=Depends(require_modulo("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Soft-delete a role. Rejects if users are assigned or if it's the default role."""
    result = await db.execute(
        select(Papel).where(Papel.id == papel_id, Papel.deletado_em.is_(None))
    )
    papel = result.scalar_one_or_none()
    if not papel:
        raise HTTPException(status_code=404, detail="Papel não encontrado")

    if papel.is_default:
        raise HTTPException(status_code=400, detail="Não é possível deletar o papel padrão")

    # Check if any users are assigned
    count_result = await db.execute(
        select(func.count(UsuarioPapel.id)).where(
            UsuarioPapel.papel_id == papel_id,
            UsuarioPapel.deletado_em.is_(None),
        )
    )
    user_count = count_result.scalar()
    if user_count and user_count > 0:
        raise HTTPException(
            status_code=400,
            detail=f"Não é possível deletar: {user_count} usuário(s) atribuído(s) a este papel",
        )

    papel.soft_delete()
    await db.flush()
    return {"status": "ok", "id": str(papel.id)}


@router.get("/papeis/modulos")
async def listar_modulos(
    _admin=Depends(require_modulo("admin")),
):
    """Return the authoritative module list for UI rendering."""
    return MODULOS


# --------------- User-Role Assignment ---------------

@router.post("/usuario-papel")
async def atribuir_papel(
    body: UsuarioPapelAssign,
    _admin=Depends(require_modulo("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Assign a role to a usuario_sei. Soft-deletes any previous assignment."""
    # Validate papel exists
    papel_result = await db.execute(
        select(Papel).where(Papel.id == body.papel_id, Papel.deletado_em.is_(None))
    )
    papel = papel_result.scalar_one_or_none()
    if not papel:
        raise HTTPException(status_code=404, detail="Papel não encontrado")

    # Soft-delete existing assignment
    existing_result = await db.execute(
        select(UsuarioPapel).where(
            UsuarioPapel.usuario_sei == body.usuario_sei,
            UsuarioPapel.deletado_em.is_(None),
        )
    )
    existing = existing_result.scalar_one_or_none()
    if existing:
        existing.soft_delete()
        await db.flush()

    # Create new assignment
    assignment = UsuarioPapel(
        usuario_sei=body.usuario_sei,
        papel_id=papel.id,
        atribuido_por=_admin.usuario_sei,
    )
    db.add(assignment)
    await db.flush()

    return {
        "status": "ok",
        "usuario_sei": body.usuario_sei,
        "papel_slug": papel.slug,
        "papel_nome": papel.nome,
    }


@router.delete("/usuario-papel/{usuario_sei}")
async def remover_papel(
    usuario_sei: str = Path(...),
    _admin=Depends(require_modulo("admin")),
    db: AsyncSession = Depends(get_db),
):
    """Remove role assignment for a usuario_sei (falls back to default role)."""
    result = await db.execute(
        select(UsuarioPapel).where(
            UsuarioPapel.usuario_sei == usuario_sei,
            UsuarioPapel.deletado_em.is_(None),
        )
    )
    assignment = result.scalar_one_or_none()
    if not assignment:
        raise HTTPException(status_code=404, detail="Atribuição não encontrada")

    assignment.soft_delete()
    await db.flush()
    return {"status": "ok", "usuario_sei": usuario_sei}
