from fastapi import APIRouter, HTTPException
from ..cache import cache
from ..models import ErrorDetail, ErrorType

router = APIRouter()


@router.get("/cache/status")
async def cache_status():
    """
    Verifica o status da conexão com o Redis.

    Returns:
        dict: Status da conexão e informações do Redis
    """
    try:
        is_available = cache.is_available()

        if not is_available:
            return {
                "status": "unavailable",
                "message": "Redis não está disponível ou não foi possível conectar",
                "connected": False
            }

        # Obter informações do Redis
        try:
            info = cache.redis_client.info()
            dbsize = cache.redis_client.dbsize()

            return {
                "status": "ok",
                "message": "Redis conectado e funcionando",
                "connected": True,
                "info": {
                    "version": info.get("redis_version", "unknown"),
                    "uptime_seconds": info.get("uptime_in_seconds", 0),
                    "used_memory_human": info.get("used_memory_human", "unknown"),
                    "total_keys": dbsize,
                    "connected_clients": info.get("connected_clients", 0)
                }
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Erro ao obter informações do Redis: {str(e)}",
                "connected": True
            }

    except Exception as e:
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
        if not cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Remove todas as chaves do banco atual
        deleted = cache.clear_pattern("*")

        return {
            "status": "ok",
            "message": "Cache resetado com sucesso",
            "keys_deleted": deleted
        }

    except HTTPException as he:
        raise he
    except Exception as e:
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
        if not cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Remove todas as chaves relacionadas ao processo
        pattern = f"processo:{numero_processo}:*"
        deleted = cache.clear_pattern(pattern)

        return {
            "status": "ok",
            "message": f"Cache do processo {numero_processo} resetado com sucesso",
            "keys_deleted": deleted,
            "processo": numero_processo
        }

    except HTTPException as he:
        raise he
    except Exception as e:
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
        if not cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Remove a chave específica do documento
        from ..cache import gerar_chave_documento
        cache_key = gerar_chave_documento(documento_formatado)
        deleted = cache.delete(cache_key)

        return {
            "status": "ok",
            "message": f"Cache do documento {documento_formatado} resetado com sucesso",
            "deleted": deleted,
            "documento_formatado": documento_formatado
        }

    except HTTPException as he:
        raise he
    except Exception as e:
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
        if not cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Lista as chaves
        keys = cache.redis_client.keys(pattern)
        total_keys = len(keys)

        # Limita o número de chaves retornadas
        limited_keys = keys[:limit]

        # Obtém TTL de cada chave
        keys_with_ttl = []
        for key in limited_keys:
            ttl = cache.redis_client.ttl(key)
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
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao listar chaves do cache",
                details={"error": str(e)}
            ).dict()
        )
