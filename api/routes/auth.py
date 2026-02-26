import base64
import json
import logging
import time

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from jwcrypto import jwe, jwk

from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


class GenerateURLRequest(BaseModel):
    id_pessoa: int
    usuario: str
    id_orgao: int
    id_pai: int | None = None
    application: str | None = None


class GenerateURLResponse(BaseModel):
    token: str
    base_url: str
    full_url: str


class RefreshTokenRequest(BaseModel):
    token: str


class RefreshTokenResponse(BaseModel):
    token: str
    expires_at: int


def _get_jwk_key() -> jwk.JWK:
    """Decode JWE_SECRET_KEY and return a JWK symmetric key."""
    key_bytes = base64.urlsafe_b64decode(settings.JWE_SECRET_KEY)
    if len(key_bytes) != 32:
        raise ValueError(f"JWE_SECRET_KEY must be 32 bytes, got {len(key_bytes)}")
    return jwk.JWK(kty="oct", k=base64.urlsafe_b64encode(key_bytes).decode().rstrip("="))


def _encrypt_payload(payload: dict) -> str:
    """Encrypt a dict payload into a compact JWE token."""
    key = _get_jwk_key()
    jwe_token = jwe.JWE(
        json.dumps(payload).encode("utf-8"),
        protected=json.dumps({"alg": "dir", "enc": "A256GCM"}),
        recipient=key,
    )
    return jwe_token.serialize(compact=True)


def _decrypt_token(token: str) -> dict:
    """Decrypt a compact JWE token and return the payload dict."""
    key = _get_jwk_key()
    jwe_token = jwe.JWE()
    jwe_token.deserialize(token, key)
    return json.loads(jwe_token.payload.decode("utf-8"))


@router.post("/generate-url", response_model=GenerateURLResponse)
async def generate_url(
    body: GenerateURLRequest,
    x_api_key: str = Header(..., alias="x-api-key"),
):
    """
    Encrypts SEI credentials into a JWE token for cookie-based auto-login.
    Called by external systems to generate pre-authenticated access URLs.
    """
    if not settings.AUTH_API_KEY:
        raise HTTPException(status_code=500, detail="AUTH_API_KEY not configured")

    if x_api_key != settings.AUTH_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    if not settings.JWE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="JWE_SECRET_KEY not configured")

    try:
        now = int(time.time())
        payload = {
            "id_pessoa": body.id_pessoa,
            "usuario": body.usuario,
            "id_orgao": body.id_orgao,
            "id_pai": body.id_pai,
            "application": body.application,
            "iat": now,
            "exp": now + settings.JWE_TOKEN_TTL,
        }

        token = _encrypt_payload(payload)
        base_url = settings.FRONTEND_BASE_URL.rstrip("/")
        full_url = f"{base_url}?token={token}"

        return GenerateURLResponse(
            token=token,
            base_url=base_url,
            full_url=full_url,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating JWE token: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to generate token")


@router.post("/refresh-token", response_model=RefreshTokenResponse)
async def refresh_token(body: RefreshTokenRequest):
    """
    Accepts a valid (non-expired) JWE token and returns a new one with
    refreshed iat/exp timestamps. No API key required — possession of a
    valid token is proof of authorization.
    """
    if not settings.JWE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="JWE_SECRET_KEY not configured")

    try:
        payload = _decrypt_token(body.token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    # Reject expired tokens
    now = int(time.time())
    if payload.get("exp", 0) < now:
        raise HTTPException(status_code=401, detail="Token expired")

    # Re-issue with fresh timestamps
    try:
        new_exp = now + settings.JWE_TOKEN_TTL
        payload["iat"] = now
        payload["exp"] = new_exp

        new_token = _encrypt_payload(payload)

        return RefreshTokenResponse(token=new_token, expires_at=new_exp)
    except Exception as e:
        logger.error(f"Error refreshing JWE token: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to refresh token")
