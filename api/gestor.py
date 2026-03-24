import logging

from .config import settings
from .sei import http_client

logger = logging.getLogger(__name__)


async def decode_jwe(token: str) -> dict | None:
    """Call the Gestor API to decode a JWE token.

    Returns the decoded payload dict, or None on any failure.
    Uses the shared http_client for connection reuse / keep-alive.
    """
    if not settings.GESTOR_API_URL:
        logger.error("GESTOR_API_URL not configured")
        return None

    url = f"{settings.GESTOR_API_URL.rstrip('/')}/embed/decode"
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if settings.GESTOR_API_TOKEN:
        headers["X-API-KEY"] = settings.GESTOR_API_TOKEN

    try:
        resp = await http_client.post(
            url,
            json={"token": token, "application": "visualizador"},
            headers=headers,
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning("Gestor decode returned %s: %s", resp.status_code, resp.text)
            return None
        return resp.json()
    except Exception as e:
        logger.error("Failed to call Gestor decode: %s", e)
        return None
