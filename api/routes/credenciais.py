"""
Rotas para gerenciamento de credenciais SEI armazenadas.
"""
import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..crypto import encrypt_password, decrypt_password
from ..models.credencial_usuario import CredencialUsuario
from ..cache import cache, gerar_chave_login
from .. import sei

logger = logging.getLogger(__name__)

router = APIRouter()

# Cache TTL: 20h (72000s) — leaves ≥4h of SEI token validity (token is valid 24h)
LOGIN_CACHE_TTL = 72000


# --------------- Request / Response schemas ---------------

class CheckCredentialsResponse(BaseModel):
    has_credentials: bool


class AutoLoginRequest(BaseModel):
    id_pessoa: int


class EmbedLoginRequest(BaseModel):
    id_pessoa: int
    cpf: str | None = None  # CPF from JWE usuario field
    usuario_sei: str
    senha: str
    orgao: str


# --------------- Helpers ---------------

async def _get_active_credential(db: AsyncSession, id_pessoa: int) -> CredencialUsuario | None:
    result = await db.execute(
        select(CredencialUsuario).where(
            CredencialUsuario.id_pessoa == id_pessoa,
            CredencialUsuario.deletado_em.is_(None),
        )
    )
    return result.scalar_one_or_none()


# --------------- Endpoints ---------------

@router.get("/check/{id_pessoa}", response_model=CheckCredentialsResponse)
async def check_credentials(id_pessoa: int, db: AsyncSession = Depends(get_db)):
    """Verifica se existem credenciais armazenadas para o usuário."""
    cred = await _get_active_credential(db, id_pessoa)
    return CheckCredentialsResponse(has_credentials=cred is not None)


@router.post("/auto-login")
async def auto_login(body: AutoLoginRequest, db: AsyncSession = Depends(get_db)):
    """
    Busca resposta de login em cache Redis. Se cache miss, decripta credenciais
    armazenadas e faz login no SEI, cacheando o resultado.
    - 401 do SEI → soft-delete das credenciais + limpa cache + retorna 401.
    - Erro de rede → não deleta, retorna 502.
    """
    # 1. Try cache first — skip DB/SEI entirely on hit
    cache_key = gerar_chave_login(body.id_pessoa)
    cached = await cache.get(cache_key)
    if cached and "response" in cached:
        logger.info(f"auto-login cache hit para id_pessoa={body.id_pessoa}, cached_at={cached.get('cached_at')}")
        return cached["response"]

    # 2. Cache miss — need credentials from DB
    logger.info(f"POST /credenciais/auto-login id_pessoa={body.id_pessoa} — cache MISS")
    cred = await _get_active_credential(db, body.id_pessoa)
    if cred is None:
        raise HTTPException(status_code=404, detail="Credenciais não encontradas")

    logger.info(f"auto-login cache miss, tentando SEI: usuario_sei={cred.usuario_sei}, orgao={cred.orgao}, id_pessoa={body.id_pessoa}")

    try:
        senha = decrypt_password(cred.senha_encrypted)
    except Exception:
        logger.error(f"Falha ao decriptar senha para id_pessoa={body.id_pessoa}")
        cred.soft_delete()
        await db.flush()
        raise HTTPException(status_code=410, detail="Credenciais corrompidas e removidas")

    # 3. Login to SEI with retries
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            data = await sei.login(cred.usuario_sei, senha, cred.orgao)
            # Include stored email so frontend uses it (not the CPF from JWE)
            data["usuario_sei"] = cred.usuario_sei
            data["orgao"] = cred.orgao
            data["papel_global"] = cred.papel_global
            data["id_pessoa"] = cred.id_pessoa

            # 4. Cache the successful response
            await cache.set(cache_key, {
                "response": data,
                "cached_at": datetime.now(timezone.utc).isoformat(),
            }, ttl=LOGIN_CACHE_TTL)
            logger.info(f"auto-login cached para id_pessoa={body.id_pessoa}")

            return data
        except HTTPException as e:
            logger.error(f"auto-login SEI falhou para id_pessoa={body.id_pessoa} (tentativa {attempt}/{max_retries}): status={e.status_code} detail={e.detail}")
            if e.status_code in (401, 422):
                cred.soft_delete()
                await db.flush()
                await cache.delete(cache_key)
                raise HTTPException(status_code=e.status_code, detail=e.detail)
            if e.status_code >= 500:
                if attempt < max_retries:
                    await asyncio.sleep(2 * attempt)
                    continue
                raise HTTPException(status_code=502, detail=f"Serviço SEI indisponível após {max_retries} tentativas: {e.detail}")
            raise
        except Exception as e:
            logger.error(f"auto-login erro inesperado para id_pessoa={body.id_pessoa} (tentativa {attempt}/{max_retries}): {type(e).__name__}: {e}")
            if attempt < max_retries:
                await asyncio.sleep(2 * attempt)
                continue
            raise HTTPException(status_code=502, detail=f"Erro inesperado após {max_retries} tentativas: {type(e).__name__}: {e}")


@router.post("/embed-login")
async def embed_login(body: EmbedLoginRequest, db: AsyncSession = Depends(get_db)):
    """
    Valida credenciais contra o SEI, criptografa a senha e armazena/atualiza no banco.
    Cacheia a resposta de login no Redis para auto-login futuro.
    Retorna a resposta raw do SEI login em caso de sucesso.
    """
    # 1. Validar contra o SEI (raises HTTPException on failure)
    data = await sei.login(body.usuario_sei, body.senha, body.orgao)

    # 2. Upsert — soft-delete existing, then insert new (preserving papel_global)
    existing = await _get_active_credential(db, body.id_pessoa)
    preserved_papel = existing.papel_global if existing else "user"
    if existing:
        existing.soft_delete()
        await db.flush()

    new_cred = CredencialUsuario(
        id_pessoa=body.id_pessoa,
        cpf=body.cpf,
        usuario_sei=body.usuario_sei,
        senha_encrypted=encrypt_password(body.senha),
        orgao=body.orgao,
        papel_global=preserved_papel,
    )
    db.add(new_cred)
    await db.flush()

    # Include email so frontend uses it (not the CPF from JWE)
    data["usuario_sei"] = body.usuario_sei
    data["orgao"] = body.orgao
    data["papel_global"] = new_cred.papel_global
    data["id_pessoa"] = body.id_pessoa

    # 3. Cache the login response for future auto-logins
    cache_key = gerar_chave_login(body.id_pessoa)
    await cache.set(cache_key, {
        "response": data,
        "cached_at": datetime.now(timezone.utc).isoformat(),
    }, ttl=LOGIN_CACHE_TTL)
    logger.info(f"embed-login cached para id_pessoa={body.id_pessoa}")

    return data
