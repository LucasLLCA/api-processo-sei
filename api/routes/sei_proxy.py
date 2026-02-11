import logging
from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel
from ..sei import login, listar_tarefa, listar_documentos, consultar_procedimento, verificar_saude
from ..cache import cache

logger = logging.getLogger(__name__)

router = APIRouter()

# Cache TTLs for proxy endpoints (1 day)
CACHE_TTL_ANDAMENTOS = 86400
CACHE_TTL_UNIDADES = 86400
CACHE_TTL_DOCUMENTOS = 86400


class LoginRequest(BaseModel):
    usuario: str
    senha: str
    orgao: str


@router.post("/login")
async def sei_login(body: LoginRequest):
    """
    Proxy para login na API SEI.
    Retorna a resposta bruta da API SEI (Token, Login, Unidades).
    """
    return await login(body.usuario, body.senha, body.orgao)


@router.get("/andamentos/{numero_processo}")
async def sei_andamentos(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
):
    """
    Proxy para buscar andamentos de um processo.
    Backend faz paginação paralela e retorna todos os andamentos.
    """
    cache_key = f"proxy:andamentos:{numero_processo}:{id_unidade}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    andamentos = await listar_tarefa(x_sei_token, numero_processo, id_unidade)

    resultado = {
        "Info": {
            "Pagina": 1,
            "TotalPaginas": 1,
            "QuantidadeItens": len(andamentos),
            "TotalItens": len(andamentos),
            "NumeroProcesso": numero_processo,
        },
        "Andamentos": andamentos,
    }

    await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
    return resultado


@router.get("/unidades-abertas/{numero_processo}")
async def sei_unidades_abertas(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
):
    """
    Proxy para consultar unidades com processo aberto.
    """
    cache_key = f"proxy:unidades:{numero_processo}:{id_unidade}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    data = await consultar_procedimento(x_sei_token, numero_processo, id_unidade)

    resultado = {
        "UnidadesProcedimentoAberto": data.get("UnidadesProcedimentoAberto", []),
        "LinkAcesso": data.get("LinkAcesso"),
    }

    await cache.set(cache_key, resultado, ttl=CACHE_TTL_UNIDADES)
    return resultado


@router.get("/documentos/{numero_processo}")
async def sei_documentos(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
):
    """
    Proxy para buscar documentos de um processo.
    Backend faz paginação paralela e retorna todos os documentos.
    """
    cache_key = f"proxy:documentos:{numero_processo}:{id_unidade}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    documentos = await listar_documentos(x_sei_token, numero_processo, id_unidade)

    resultado = {
        "Info": {
            "Pagina": 1,
            "TotalPaginas": 1,
            "QuantidadeItens": len(documentos),
            "TotalItens": len(documentos),
        },
        "Documentos": documentos,
    }

    await cache.set(cache_key, resultado, ttl=CACHE_TTL_DOCUMENTOS)
    return resultado


@router.delete("/cache/{numero_processo}")
async def sei_invalidar_cache(numero_processo: str):
    """
    Invalida todo o cache proxy de um processo específico.
    Remove andamentos, unidades e documentos cacheados.
    """
    deleted = 0
    for pattern in [
        f"proxy:andamentos:{numero_processo}:*",
        f"proxy:unidades:{numero_processo}:*",
        f"proxy:documentos:{numero_processo}:*",
    ]:
        deleted += await cache.clear_pattern(pattern)

    logger.info(f"Cache proxy invalidado para processo {numero_processo}: {deleted} chaves removidas")

    return {
        "status": "ok",
        "message": f"Cache proxy do processo {numero_processo} invalidado",
        "keys_deleted": deleted,
    }


@router.get("/health")
async def sei_health():
    """
    Verifica saúde da API SEI.
    """
    return await verificar_saude()
