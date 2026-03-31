"""
Middleware para registro automatico de atividades dos usuarios.
Intercepta requisicoes e loga no banco de dados de forma assincrona.
"""
import asyncio
import logging
import re
import time
from urllib.parse import parse_qs, urlparse

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from .database import AsyncSessionLocal
from .models.registro_atividade import RegistroAtividade

logger = logging.getLogger(__name__)

# Rotas ignoradas (nao geram log)
SKIP_PATHS = {"/", "/docs", "/redoc", "/openapi.json", "/test-ia"}
SKIP_PREFIXES = ("/admin/analytics/",)

# Mapeamento rota -> tipo_atividade
ROUTE_TYPE_MAP = [
    (re.compile(r"^/auth/"), "login"),
    (re.compile(r"^/sei-proxy/login"), "login"),
    (re.compile(r"^/processo/[^/]+/andamentos"), "visualizar_processo"),
    (re.compile(r"^/processo/[^/]+/documentos"), "visualizar_documento"),
    (re.compile(r"^/processo/[^/]+/resumo"), "gerar_resumo"),
    (re.compile(r"^/processo/"), "visualizar_processo"),
    (re.compile(r"^/historico"), "pesquisar_processo"),
    (re.compile(r"^/observacoes"), "criar_observacao"),
    (re.compile(r"^/fluxos"), "fluxo"),
    (re.compile(r"^/compartilhamentos"), "compartilhar"),
    (re.compile(r"^/equipes"), "equipe"),
    (re.compile(r"^/tags"), "tag"),
    (re.compile(r"^/admin/"), "admin_action"),
    (re.compile(r"^/d1/"), "consulta_d1"),
]

# Cache de orgao por usuario_sei (evita JOINs repetidos)
_orgao_cache: dict[str, str] = {}


def _classify_activity(path: str, method: str) -> str:
    """Classifica a atividade com base na rota e metodo."""
    for pattern, tipo in ROUTE_TYPE_MAP:
        if pattern.match(path):
            return tipo
    return "outro"


def _extract_recurso(path: str) -> str | None:
    """Extrai o recurso principal da URL (ex: numero do processo)."""
    match = re.match(r"^/processo/([^/]+)", path)
    if match:
        return match.group(1)
    match = re.match(r"^/fluxos/([^/]+)", path)
    if match:
        return match.group(1)
    match = re.match(r"^/equipes/([^/]+)", path)
    if match:
        return match.group(1)
    return None


async def _get_orgao(usuario_sei: str) -> str | None:
    """Busca o orgao do usuario, com cache em memoria."""
    if usuario_sei in _orgao_cache:
        return _orgao_cache[usuario_sei]
    try:
        from .models.credencial_usuario import CredencialUsuario
        from sqlalchemy import select
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(CredencialUsuario.orgao).where(
                    CredencialUsuario.usuario_sei == usuario_sei,
                    CredencialUsuario.deletado_em.is_(None),
                ).limit(1)
            )
            orgao = result.scalar_one_or_none()
            if orgao:
                _orgao_cache[usuario_sei] = orgao
            return orgao
    except Exception:
        return None


async def _log_atividade(
    usuario_sei: str,
    tipo_atividade: str,
    recurso: str | None,
    rota: str,
    metodo_http: str,
    status_code: int,
    duracao_ms: int,
    ip_address: str | None,
    orgao: str | None,
):
    """Grava o registro de atividade no banco (fire-and-forget)."""
    try:
        async with AsyncSessionLocal() as session:
            atividade = RegistroAtividade(
                usuario_sei=usuario_sei,
                tipo_atividade=tipo_atividade,
                recurso=recurso,
                rota=rota,
                metodo_http=metodo_http,
                status_code=status_code,
                duracao_ms=duracao_ms,
                ip_address=ip_address,
                orgao=orgao,
            )
            session.add(atividade)
            await session.commit()
    except Exception as e:
        logger.warning(f"Falha ao registrar atividade: {e}")


class AtividadeMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        path = request.url.path

        # Skip paths que nao devem ser logados
        if path in SKIP_PATHS or any(path.startswith(p) for p in SKIP_PREFIXES):
            return await call_next(request)

        start = time.monotonic()
        response = await call_next(request)
        duracao_ms = int((time.monotonic() - start) * 1000)

        # Extrair usuario_sei do query param
        usuario_sei = request.query_params.get("usuario_sei")
        if not usuario_sei:
            return response

        # Classificar e registrar (fire-and-forget)
        tipo = _classify_activity(path, request.method)
        recurso = _extract_recurso(path)
        ip = request.client.host if request.client else None
        orgao = await _get_orgao(usuario_sei)

        asyncio.create_task(
            _log_atividade(
                usuario_sei=usuario_sei,
                tipo_atividade=tipo,
                recurso=recurso,
                rota=path,
                metodo_http=request.method,
                status_code=response.status_code,
                duracao_ms=duracao_ms,
                ip_address=ip,
                orgao=orgao,
            )
        )

        return response
