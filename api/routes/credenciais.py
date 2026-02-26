"""
Rotas para gerenciamento de credenciais SEI armazenadas.
"""
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..crypto import encrypt_password, decrypt_password
from ..models.credencial_usuario import CredencialUsuario
from .. import sei

logger = logging.getLogger(__name__)

router = APIRouter()


# --------------- Request / Response schemas ---------------

class CheckCredentialsResponse(BaseModel):
    has_credentials: bool


class AutoLoginRequest(BaseModel):
    id_pessoa: int


class EmbedLoginRequest(BaseModel):
    id_pessoa: int
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
    Busca credenciais armazenadas, decripta e faz login no SEI.
    - 401 do SEI → soft-delete das credenciais + retorna 401.
    - Erro de rede → não deleta, retorna 502.
    """
    cred = await _get_active_credential(db, body.id_pessoa)
    if cred is None:
        raise HTTPException(status_code=404, detail="Credenciais não encontradas")

    try:
        senha = decrypt_password(cred.senha_encrypted)
    except Exception:
        logger.error(f"Falha ao decriptar senha para id_pessoa={body.id_pessoa}")
        cred.soft_delete()
        await db.flush()
        raise HTTPException(status_code=410, detail="Credenciais corrompidas e removidas")

    try:
        data = await sei.login(cred.usuario_sei, senha, cred.orgao)
        return data
    except HTTPException as e:
        if e.status_code == 401:
            # Credenciais inválidas (senha alterada, etc) — soft delete
            cred.soft_delete()
            await db.flush()
            raise HTTPException(status_code=401, detail=e.detail)
        if e.status_code >= 500:
            # Erro do serviço SEI — não deletar credenciais
            raise HTTPException(status_code=502, detail="Serviço SEI indisponível")
        raise


@router.post("/embed-login")
async def embed_login(body: EmbedLoginRequest, db: AsyncSession = Depends(get_db)):
    """
    Valida credenciais contra o SEI, criptografa a senha e armazena/atualiza no banco.
    Retorna a resposta raw do SEI login em caso de sucesso.
    """
    # 1. Validar contra o SEI (raises HTTPException on failure)
    data = await sei.login(body.usuario_sei, body.senha, body.orgao)

    # 2. Upsert — soft-delete existing, then insert new
    existing = await _get_active_credential(db, body.id_pessoa)
    if existing:
        existing.soft_delete()
        await db.flush()

    new_cred = CredencialUsuario(
        id_pessoa=body.id_pessoa,
        usuario_sei=body.usuario_sei,
        senha_encrypted=encrypt_password(body.senha),
        orgao=body.orgao,
    )
    db.add(new_cred)
    await db.flush()

    return data
