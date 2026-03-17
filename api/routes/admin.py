import logging
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query
from pydantic import BaseModel
from sqlalchemy import select, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession

from ..cache import cache, gerar_chave_documento
from ..database import get_db
from ..models.credencial_usuario import CredencialUsuario
from ..models.configuracao_horas import ConfiguracaoHorasAndamento
from ..schemas_legacy import ErrorDetail, ErrorType

logger = logging.getLogger(__name__)

router = APIRouter()


# --------------- Admin guard ---------------

async def require_admin(admin_id_pessoa: int = Query(..., alias="id_pessoa"), db: AsyncSession = Depends(get_db)):
    """Dependency that verifies the requesting user has papel_global='admin'."""
    result = await db.execute(
        select(CredencialUsuario).where(
            CredencialUsuario.id_pessoa == admin_id_pessoa,
            CredencialUsuario.deletado_em.is_(None),
        )
    )
    cred = result.scalar_one_or_none()
    if not cred or cred.papel_global != "admin":
        raise HTTPException(status_code=403, detail="Acesso restrito a administradores")
    return cred


# --------------- Schemas ---------------

class UpdatePapelRequest(BaseModel):
    papel_global: str  # "admin" | "beta" | "user"


class HorasItem(BaseModel):
    grupo_key: str
    horas: float


class SaveConfiguracaoHorasRequest(BaseModel):
    orgao: str
    items: List[HorasItem]


class UsuarioResponse(BaseModel):
    id_pessoa: int
    usuario_sei: str
    orgao: str
    papel_global: str
    cpf: Optional[str] = None


# --------------- User role endpoints ---------------

@router.get("/usuarios")
async def listar_usuarios(
    search: str = Query(default=""),
    _admin: CredencialUsuario = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List all users with their roles (active credentials only)."""
    query = select(CredencialUsuario).where(CredencialUsuario.deletado_em.is_(None))
    if search.strip():
        pattern = f"%{search.strip()}%"
        query = query.where(
            CredencialUsuario.usuario_sei.ilike(pattern)
            | CredencialUsuario.orgao.ilike(pattern)
        )
    query = query.order_by(CredencialUsuario.usuario_sei)
    result = await db.execute(query)
    rows = result.scalars().all()
    return [
        UsuarioResponse(
            id_pessoa=r.id_pessoa,
            usuario_sei=r.usuario_sei,
            orgao=r.orgao,
            papel_global=r.papel_global,
            cpf=r.cpf,
        )
        for r in rows
    ]


@router.patch("/usuarios/{id_pessoa}/papel")
async def atualizar_papel(
    body: UpdatePapelRequest,
    id_pessoa: int = Path(..., description="ID da pessoa"),
    _admin: CredencialUsuario = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update a user's global role."""
    if body.papel_global not in ("admin", "beta", "user"):
        raise HTTPException(status_code=400, detail="Papel inválido. Use: admin, beta, user")

    result = await db.execute(
        select(CredencialUsuario).where(
            CredencialUsuario.id_pessoa == id_pessoa,
            CredencialUsuario.deletado_em.is_(None),
        )
    )
    target = result.scalar_one_or_none()
    if not target:
        raise HTTPException(status_code=404, detail="Usuário não encontrado")

    target.papel_global = body.papel_global
    target.atualizado_em = datetime.now(timezone.utc)
    await db.flush()

    return {"status": "ok", "id_pessoa": id_pessoa, "papel_global": body.papel_global}


# --------------- Hour coefficient endpoints ---------------

@router.get("/configuracao-horas")
async def get_configuracao_horas(
    orgao: str = Query(...),
    _admin: CredencialUsuario = Depends(require_admin),
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
    _admin: CredencialUsuario = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Bulk upsert hour coefficients for an orgao."""
    now = datetime.now(timezone.utc)
    admin_user = _admin.usuario_sei

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
    _admin: CredencialUsuario = Depends(require_admin),
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
