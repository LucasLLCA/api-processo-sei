import base64
import json
import logging

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from jwcrypto import jwe, jwk

from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


class GenerateURLRequest(BaseModel):
    email: str
    password: str
    orgao: str


class GenerateURLResponse(BaseModel):
    token: str
    base_url: str
    full_url: str


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
        # Decode the base64url-encoded 256-bit key
        key_bytes = base64.urlsafe_b64decode(settings.JWE_SECRET_KEY)
        if len(key_bytes) != 32:
            raise ValueError(f"JWE_SECRET_KEY must be 32 bytes, got {len(key_bytes)}")

        # Create JWK symmetric key
        key = jwk.JWK(kty="oct", k=base64.urlsafe_b64encode(key_bytes).decode().rstrip("="))

        # Create JWE token with dir + A256GCM
        payload = json.dumps({
            "email": body.email,
            "password": body.password,
            "orgao": body.orgao,
        })

        jwe_token = jwe.JWE(
            payload.encode("utf-8"),
            protected=json.dumps({
                "alg": "dir",
                "enc": "A256GCM",
            }),
            recipient=key,
        )
        token = jwe_token.serialize(compact=True)

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
