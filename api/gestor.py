import logging

import httpx

from .config import settings

logger = logging.getLogger(__name__)


async def decode_jwe(token: str) -> dict | None:
    """Call the Gestor API to decode a JWE token.

    Returns the decoded payload dict, or None on any failure.
    """
    if not settings.GESTOR_API_URL:
        logger.error("GESTOR_API_URL not configured")
        return None

    url = f"{settings.GESTOR_API_URL.rstrip('/')}/embed/decode"
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if settings.GESTOR_API_TOKEN:
        headers["X-API-KEY"] = settings.GESTOR_API_TOKEN

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json={"token": token, "application": "visualizador"}, headers=headers)
        if resp.status_code != 200:
            logger.warning("Gestor decode returned %s: %s", resp.status_code, resp.text)
            return None
        return resp.json()
    except Exception as e:
        logger.error("Failed to call Gestor decode: %s", e)
        return None
