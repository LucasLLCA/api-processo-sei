import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from ..cache import cache, gerar_chave_documento
from ..database import get_db
from ..models.credencial_usuario import CredencialUsuario
from ..models.configuracao_horas import ConfiguracaoHorasAndamento
from ..models.papel import Papel
from ..models.usuario_papel import UsuarioPapel
from ..schemas_legacy import ErrorDetail, ErrorType
from ..rbac import require_modulo

logger = logging.getLogger(__name__)

router = APIRouter()


# --------------- Admin guard (RBAC-based) ---------------

require_admin = require_modulo("admin")


# --------------- Schemas ---------------

class HorasItem(BaseModel):
    grupo_key: str
    horas: float


class SaveConfiguracaoHorasRequest(BaseModel):
    orgao: str
    items: List[HorasItem]


class UsuarioResponse(BaseModel):
    usuario_sei: str
    orgao: str
    papel_nome: Optional[str] = None
    papel_slug: Optional[str] = None
    papel_id: Optional[str] = None


class UsuariosPaginatedResponse(BaseModel):
    items: List[UsuarioResponse]
    total: int
    page: int
    page_size: int


# --------------- User role endpoints ---------------

@router.get("/usuarios")
async def listar_usuarios(
    search: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    _admin: str = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List users with their roles, grouped by unique email, paginated."""
    # Subquery: distinct emails with their most recent orgao
    base = (
        select(
            CredencialUsuario.usuario_sei,
            func.max(CredencialUsuario.orgao).label("orgao"),
        )
        .where(CredencialUsuario.deletado_em.is_(None))
        .group_by(CredencialUsuario.usuario_sei)
    )
    if search.strip():
        pattern = f"%{search.strip()}%"
        base = base.having(
            CredencialUsuario.usuario_sei.ilike(pattern)
            | func.max(CredencialUsuario.orgao).ilike(pattern)
        )
    users_sub = base.subquery()

    # Count total
    count_result = await db.execute(select(func.count()).select_from(users_sub))
    total = count_result.scalar() or 0

    # Paginated query with role join
    query = (
        select(users_sub.c.usuario_sei, users_sub.c.orgao, Papel)
        .outerjoin(
            UsuarioPapel,
            (UsuarioPapel.usuario_sei == users_sub.c.usuario_sei)
            & (UsuarioPapel.deletado_em.is_(None)),
        )
        .outerjoin(
            Papel,
            (Papel.id == UsuarioPapel.papel_id) & (Papel.deletado_em.is_(None)),
        )
        .order_by(users_sub.c.usuario_sei)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(query)
    rows = result.all()

    return UsuariosPaginatedResponse(
        items=[
            UsuarioResponse(
                usuario_sei=usuario_sei,
                orgao=orgao,
                papel_nome=papel.nome if papel else None,
                papel_slug=papel.slug if papel else None,
                papel_id=str(papel.id) if papel else None,
            )
            for usuario_sei, orgao, papel in rows
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


# --------------- Hour coefficient endpoints ---------------

@router.get("/configuracao-horas")
async def get_configuracao_horas(
    orgao: str = Query(...),
    _admin: str = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get all hour coefficients for an orgao (admin)."""
    result = await db.execute(
        select(ConfiguracaoHorasAndamento).where(
            ConfiguracaoHorasAndamento.orgao == orgao
        ).order_by(ConfiguracaoHorasAndamento.grupo_key)
    )
    rows = result.scalars().all()
    return [
        {
            "grupo_key": r.grupo_key,
            "horas": r.horas,
            "atualizado_em": r.atualizado_em.isoformat() if r.atualizado_em else None,
            "atualizado_por": r.atualizado_por,
        }
        for r in rows
    ]


@router.put("/configuracao-horas")
async def save_configuracao_horas(
    body: SaveConfiguracaoHorasRequest,
    _admin: str = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Bulk upsert hour coefficients for an orgao."""
    now = datetime.now(timezone.utc)
    admin_user = _admin

    for item in body.items:
        result = await db.execute(
            select(ConfiguracaoHorasAndamento).where(
                ConfiguracaoHorasAndamento.orgao == body.orgao,
                ConfiguracaoHorasAndamento.grupo_key == item.grupo_key,
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            existing.horas = item.horas
            existing.atualizado_em = now
            existing.atualizado_por = admin_user
        else:
            db.add(ConfiguracaoHorasAndamento(
                orgao=body.orgao,
                grupo_key=item.grupo_key,
                horas=item.horas,
                atualizado_em=now,
                atualizado_por=admin_user,
            ))

    await db.flush()
    return {"status": "ok", "orgao": body.orgao, "items_saved": len(body.items)}


@router.get("/orgaos")
async def listar_orgaos(
    _admin: str = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List distinct orgaos from active credentials."""
    result = await db.execute(
        select(distinct(CredencialUsuario.orgao)).where(
            CredencialUsuario.deletado_em.is_(None)
        ).order_by(CredencialUsuario.orgao)
    )
    return [row[0] for row in result.all()]


@router.get("/cache/status")
async def cache_status():
    """
    Verifica o status da conexão com o Redis.

    Returns:
        dict: Status da conexão e informações do Redis
    """
    try:
        is_available = await cache.is_available()

        if not is_available:
            return {
                "status": "unavailable",
                "message": "Redis não está disponível ou não foi possível conectar",
                "connected": False
            }

        # Obter informações do Redis
        try:
            info = await cache.get_info()
            keys = await cache.get_keys("*")

            return {
                "status": "ok",
                "message": "Redis conectado e funcionando",
                "connected": True,
                "info": {
                    "used_memory_human": info.get("used_memory_human", "unknown"),
                    "total_keys": len(keys),
                    "connected_clients": info.get("connected_clients", 0),
                    "keyspace_hits": info.get("keyspace_hits", 0),
                    "keyspace_misses": info.get("keyspace_misses", 0)
                }
            }
        except Exception as e:
            logger.error(f"Erro ao obter informações do Redis: {str(e)}")
            return {
                "status": "error",
                "message": f"Erro ao obter informações do Redis: {str(e)}",
                "connected": True
            }

    except Exception as e:
        logger.error(f"Erro ao verificar status do cache: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao verificar status do cache",
                details={"error": str(e)}
            ).dict()
        )


@router.delete("/cache/reset")
async def reset_cache():
    """
    Reseta todo o cache (remove todas as chaves).

    Returns:
        dict: Resultado da operação
    """
    try:
        if not await cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Remove todas as chaves do banco atual
        deleted = await cache.clear_pattern("*")

        logger.info(f"Cache resetado: {deleted} chaves removidas")

        return {
            "status": "ok",
            "message": "Cache resetado com sucesso",
            "keys_deleted": deleted
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao resetar cache: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao resetar cache",
                details={"error": str(e)}
            ).dict()
        )


@router.delete("/cache/processo/{numero_processo}")
async def reset_cache_processo(numero_processo: str):
    """
    Reseta o cache de um processo específico.
    Remove todas as chaves relacionadas ao processo.

    Args:
        numero_processo (str): Número do processo

    Returns:
        dict: Resultado da operação
    """
    try:
        if not await cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Remove todas as chaves relacionadas ao processo (incluindo proxy cache)
        deleted = 0
        for pattern in [
            f"processo:{numero_processo}:*",
            f"andamento:{numero_processo}",
            f"resumo:{numero_processo}",
            f"proxy:andamentos:{numero_processo}:*",
            f"proxy:unidades:{numero_processo}:*",
            f"proxy:documentos:{numero_processo}:*",
        ]:
            deleted += await cache.clear_pattern(pattern)

        logger.info(f"Cache do processo {numero_processo} resetado: {deleted} chaves removidas")

        return {
            "status": "ok",
            "message": f"Cache do processo {numero_processo} resetado com sucesso",
            "keys_deleted": deleted,
            "processo": numero_processo
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao resetar cache do processo: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao resetar cache do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )


@router.delete("/cache/documento/{documento_formatado}")
async def reset_cache_documento(documento_formatado: str):
    """
    Reseta o cache de um documento específico.

    Args:
        documento_formatado (str): Número do documento formatado

    Returns:
        dict: Resultado da operação
    """
    try:
        if not await cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Remove a chave específica do documento
        cache_key = gerar_chave_documento(documento_formatado)
        deleted = await cache.delete(cache_key)

        logger.info(f"Cache do documento {documento_formatado} resetado")

        return {
            "status": "ok",
            "message": f"Cache do documento {documento_formatado} resetado com sucesso",
            "deleted": deleted,
            "documento_formatado": documento_formatado
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao resetar cache do documento: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao resetar cache do documento",
                details={"error": str(e), "documento_formatado": documento_formatado}
            ).dict()
        )


@router.get("/cache/keys")
async def list_cache_keys(pattern: str = "*", limit: int = 100):
    """
    Lista as chaves do cache que correspondem ao padrão.

    Args:
        pattern (str): Padrão para filtrar chaves (padrão: "*" - todas)
        limit (int): Número máximo de chaves a retornar (padrão: 100)

    Returns:
        dict: Lista de chaves encontradas
    """
    try:
        if not await cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Lista as chaves usando SCAN
        keys = await cache.get_keys(pattern)
        total_keys = len(keys)

        # Limita o número de chaves retornadas
        limited_keys = keys[:limit]

        # Obtém TTL de cada chave
        keys_with_ttl = []
        for key in limited_keys:
            ttl = await cache.redis_client.ttl(key)
            keys_with_ttl.append({
                "key": key,
                "ttl": ttl if ttl > 0 else "sem expiração" if ttl == -1 else "expirado"
            })

        return {
            "status": "ok",
            "pattern": pattern,
            "total_keys": total_keys,
            "returned_keys": len(limited_keys),
            "limit": limit,
            "keys": keys_with_ttl
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao listar chaves do cache: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao listar chaves do cache",
                details={"error": str(e)}
            ).dict()
        )
