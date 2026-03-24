"""
Proxy para a API D-1 (api-sei-atividades).

Roteia requisições do frontend para o serviço D-1 interno,
evitando mixed-content (HTTPS→HTTP) no navegador.
"""
import logging
from fastapi import APIRouter, HTTPException
from fastapi.responses import ORJSONResponse
from ..sei import http_client
from ..config import settings

router = APIRouter()
logger = logging.getLogger(__name__)

D1_TIMEOUT = 8  # seconds (frontend has 5s, give backend a bit more)


@router.get("/processo/{numero:path}/andamentos")
async def d1_andamentos(numero: str):
    """Proxy: GET /d1/processo/{numero}/andamentos → D-1 API."""
    if not settings.D1_API_URL:
        raise HTTPException(status_code=503, detail="D1_API_URL não configurada")

    url = f"{settings.D1_API_URL.rstrip('/')}/api/v1/processo/{numero}/andamentos"

    try:
        resp = await http_client.get(url, timeout=D1_TIMEOUT)
    except Exception as e:
        logger.warning(f"D-1 proxy falhou: {e}")
        raise HTTPException(status_code=502, detail=f"D-1 indisponível: {e}")

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    return ORJSONResponse(content=resp.json(), status_code=200)


# ─── BI proxy endpoints ──────────────────────────────────────────────────

BI_TIMEOUT = 30  # BI queries can be heavier

async def _proxy_bi_get(path: str, params: dict | None = None):
    """Generic GET proxy to D-1 BI endpoints."""
    if not settings.D1_API_URL:
        raise HTTPException(status_code=503, detail="D1_API_URL não configurada")
    url = f"{settings.D1_API_URL.rstrip('/')}/api/v1/bi/{path}"
    try:
        resp = await http_client.get(url, params=params, timeout=BI_TIMEOUT)
    except Exception as e:
        logger.warning(f"D-1 BI proxy falhou ({path}): {e}")
        raise HTTPException(status_code=502, detail=f"D-1 BI indisponível: {e}")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return ORJSONResponse(content=resp.json(), status_code=200)


@router.get("/bi/estoque-processos")
async def bi_estoque_processos(
    unidade_origem: str | None = None,
    unidade_passagem: str | None = None,
    unidade_aberta: str | None = None,
    orgao_origem: str | None = None,
    orgao_passagem: str | None = None,
    orgao_aberta: str | None = None,
):
    """Proxy: GET /d1/bi/estoque-processos → D-1 API."""
    params = {}
    if unidade_origem:
        params["unidade_origem"] = unidade_origem
    if unidade_passagem:
        params["unidade_passagem"] = unidade_passagem
    if unidade_aberta:
        params["unidade_aberta"] = unidade_aberta
    if orgao_origem:
        params["orgao_origem"] = orgao_origem
    if orgao_passagem:
        params["orgao_passagem"] = orgao_passagem
    if orgao_aberta:
        params["orgao_aberta"] = orgao_aberta
    return await _proxy_bi_get("estoque-processos", params or None)


@router.get("/bi/estoque-processos/list")
async def bi_estoque_list(
    page: int = 1,
    page_size: int = 20,
    search: str | None = None,
    unidade_origem: str | None = None,
    unidade_passagem: str | None = None,
    unidade_aberta: str | None = None,
    orgao_origem: str | None = None,
    orgao_passagem: str | None = None,
    orgao_aberta: str | None = None,
):
    """Proxy: GET /d1/bi/estoque-processos/list → D-1 API."""
    params: dict = {"page": str(page), "page_size": str(page_size)}
    if search:
        params["search"] = search
    if unidade_origem:
        params["unidade_origem"] = unidade_origem
    if unidade_passagem:
        params["unidade_passagem"] = unidade_passagem
    if unidade_aberta:
        params["unidade_aberta"] = unidade_aberta
    if orgao_origem:
        params["orgao_origem"] = orgao_origem
    if orgao_passagem:
        params["orgao_passagem"] = orgao_passagem
    if orgao_aberta:
        params["orgao_aberta"] = orgao_aberta
    return await _proxy_bi_get("estoque-processos/list", params)


@router.get("/bi/estoque-processos/unidades")
async def bi_estoque_unidades():
    """Proxy: GET /d1/bi/estoque-processos/unidades → D-1 API."""
    return await _proxy_bi_get("estoque-processos/unidades")


@router.post("/bi/estoque-processos/refresh")
async def bi_estoque_refresh():
    """Proxy: POST /d1/bi/estoque-processos/refresh → D-1 API."""
    if not settings.D1_API_URL:
        raise HTTPException(status_code=503, detail="D1_API_URL não configurada")
    url = f"{settings.D1_API_URL.rstrip('/')}/api/v1/bi/estoque-processos/refresh"
    try:
        resp = await http_client.post(url, timeout=BI_TIMEOUT)
    except Exception as e:
        logger.warning(f"D-1 BI proxy falhou (refresh): {e}")
        raise HTTPException(status_code=502, detail=f"D-1 BI indisponível: {e}")
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)
    return ORJSONResponse(content=resp.json(), status_code=200)


@router.get("/bi/tasks")
async def bi_tasks():
    """Proxy: GET /d1/bi/tasks → D-1 API."""
    return await _proxy_bi_get("tasks")


@router.get("/bi/tasks/{task_id}")
async def bi_task_status(task_id: str):
    """Proxy: GET /d1/bi/tasks/{task_id} → D-1 API."""
    return await _proxy_bi_get(f"tasks/{task_id}")
