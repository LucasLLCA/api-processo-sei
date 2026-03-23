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
