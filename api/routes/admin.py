import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

import orjson

from ..cache import cache, gerar_chave_documento
from ..database import get_db
from ..models.credencial_usuario import CredencialUsuario
from ..models.configuracao_horas import ConfiguracaoHorasAndamento
from ..models.historico_pesquisa import HistoricoPesquisa
from ..models.papel import Papel
from ..models.registro_atividade import RegistroAtividade
from ..models.usuario_papel import UsuarioPapel
from ..schemas_legacy import ErrorDetail, ErrorType
from ..rbac import require_modulo

logger = logging.getLogger(__name__)

router = APIRouter()


# --------------- Admin guard (RBAC-based) ---------------

require_admin = require_modulo("admin")


# --------------- Schemas ---------------

class HorasItem(BaseModel):
    grupo_key: str
    horas: float


class SaveConfiguracaoHorasRequest(BaseModel):
    orgao: str
    items: List[HorasItem]


class UsuarioResponse(BaseModel):
    usuario_sei: str
    orgao: str
    papel_nome: Optional[str] = None
    papel_slug: Optional[str] = None
    papel_id: Optional[str] = None


class UsuariosPaginatedResponse(BaseModel):
    items: List[UsuarioResponse]
    total: int
    page: int
    page_size: int


# --------------- User role endpoints ---------------

@router.get("/usuarios")
async def listar_usuarios(
    search: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=100),
    _admin: str = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List users with their roles, grouped by unique email, paginated."""
    # Subquery: distinct emails with their most recent orgao
    base = (
        select(
            CredencialUsuario.usuario_sei,
            func.max(CredencialUsuario.orgao).label("orgao"),
        )
        .where(CredencialUsuario.deletado_em.is_(None))
        .group_by(CredencialUsuario.usuario_sei)
    )
    if search.strip():
        pattern = f"%{search.strip()}%"
        base = base.having(
            CredencialUsuario.usuario_sei.ilike(pattern)
            | func.max(CredencialUsuario.orgao).ilike(pattern)
        )
    users_sub = base.subquery()

    # Count total
    count_result = await db.execute(select(func.count()).select_from(users_sub))
    total = count_result.scalar() or 0

    # Paginated query with role join
    query = (
        select(users_sub.c.usuario_sei, users_sub.c.orgao, Papel)
        .outerjoin(
            UsuarioPapel,
            (UsuarioPapel.usuario_sei == users_sub.c.usuario_sei)
            & (UsuarioPapel.deletado_em.is_(None)),
        )
        .outerjoin(
            Papel,
            (Papel.id == UsuarioPapel.papel_id) & (Papel.deletado_em.is_(None)),
        )
        .order_by(users_sub.c.usuario_sei)
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(query)
    rows = result.all()

    return UsuariosPaginatedResponse(
        items=[
            UsuarioResponse(
                usuario_sei=usuario_sei,
                orgao=orgao,
                papel_nome=papel.nome if papel else None,
                papel_slug=papel.slug if papel else None,
                papel_id=str(papel.id) if papel else None,
            )
            for usuario_sei, orgao, papel in rows
        ],
        total=total,
        page=page,
        page_size=page_size,
    )


# --------------- Hour coefficient endpoints ---------------

@router.get("/configuracao-horas")
async def get_configuracao_horas(
    orgao: str = Query(...),
    _admin: str = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get all hour coefficients for an orgao (admin)."""
    result = await db.execute(
        select(ConfiguracaoHorasAndamento).where(
            ConfiguracaoHorasAndamento.orgao == orgao
        ).order_by(ConfiguracaoHorasAndamento.grupo_key)
    )
    rows = result.scalars().all()
    return [
        {
            "grupo_key": r.grupo_key,
            "horas": r.horas,
            "atualizado_em": r.atualizado_em.isoformat() if r.atualizado_em else None,
            "atualizado_por": r.atualizado_por,
        }
        for r in rows
    ]


@router.put("/configuracao-horas")
async def save_configuracao_horas(
    body: SaveConfiguracaoHorasRequest,
    _admin: str = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Bulk upsert hour coefficients for an orgao."""
    now = datetime.now(timezone.utc)
    admin_user = _admin

    for item in body.items:
        result = await db.execute(
            select(ConfiguracaoHorasAndamento).where(
                ConfiguracaoHorasAndamento.orgao == body.orgao,
                ConfiguracaoHorasAndamento.grupo_key == item.grupo_key,
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            existing.horas = item.horas
            existing.atualizado_em = now
            existing.atualizado_por = admin_user
        else:
            db.add(ConfiguracaoHorasAndamento(
                orgao=body.orgao,
                grupo_key=item.grupo_key,
                horas=item.horas,
                atualizado_em=now,
                atualizado_por=admin_user,
            ))

    await db.flush()
    return {"status": "ok", "orgao": body.orgao, "items_saved": len(body.items)}


@router.get("/orgaos")
async def listar_orgaos(
    _admin: str = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List distinct orgaos from active credentials."""
    result = await db.execute(
        select(distinct(CredencialUsuario.orgao)).where(
            CredencialUsuario.deletado_em.is_(None)
        ).order_by(CredencialUsuario.orgao)
    )
    return [row[0] for row in result.all()]


@router.get("/cache/status")
async def cache_status():
    """
    Verifica o status da conexão com o Redis.

    Returns:
        dict: Status da conexão e informações do Redis
    """
    try:
        is_available = await cache.is_available()

        if not is_available:
            return {
                "status": "unavailable",
                "message": "Redis não está disponível ou não foi possível conectar",
                "connected": False
            }

        # Obter informações do Redis
        try:
            info = await cache.get_info()
            keys = await cache.get_keys("*")

            return {
                "status": "ok",
                "message": "Redis conectado e funcionando",
                "connected": True,
                "info": {
                    "used_memory_human": info.get("used_memory_human", "unknown"),
                    "total_keys": len(keys),
                    "connected_clients": info.get("connected_clients", 0),
                    "keyspace_hits": info.get("keyspace_hits", 0),
                    "keyspace_misses": info.get("keyspace_misses", 0)
                }
            }
        except Exception as e:
            logger.error(f"Erro ao obter informações do Redis: {str(e)}")
            return {
                "status": "error",
                "message": f"Erro ao obter informações do Redis: {str(e)}",
                "connected": True
            }

    except Exception as e:
        logger.error(f"Erro ao verificar status do cache: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao verificar status do cache",
                details={"error": str(e)}
            ).dict()
        )


@router.delete("/cache/reset")
async def reset_cache():
    """
    Reseta todo o cache (remove todas as chaves).

    Returns:
        dict: Resultado da operação
    """
    try:
        if not await cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Remove todas as chaves do banco atual
        deleted = await cache.clear_pattern("*")

        logger.info(f"Cache resetado: {deleted} chaves removidas")

        return {
            "status": "ok",
            "message": "Cache resetado com sucesso",
            "keys_deleted": deleted
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao resetar cache: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao resetar cache",
                details={"error": str(e)}
            ).dict()
        )


@router.delete("/cache/processo/{numero_processo}")
async def reset_cache_processo(numero_processo: str):
    """
    Reseta o cache de um processo específico.
    Remove todas as chaves relacionadas ao processo.

    Args:
        numero_processo (str): Número do processo

    Returns:
        dict: Resultado da operação
    """
    try:
        if not await cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Remove todas as chaves relacionadas ao processo (incluindo proxy cache)
        deleted = 0
        for pattern in [
            f"processo:{numero_processo}:*",
            f"andamento:{numero_processo}",
            f"resumo:{numero_processo}",
            f"proxy:andamentos:{numero_processo}:*",
            f"proxy:unidades:{numero_processo}:*",
            f"proxy:documentos:{numero_processo}:*",
        ]:
            deleted += await cache.clear_pattern(pattern)

        logger.info(f"Cache do processo {numero_processo} resetado: {deleted} chaves removidas")

        return {
            "status": "ok",
            "message": f"Cache do processo {numero_processo} resetado com sucesso",
            "keys_deleted": deleted,
            "processo": numero_processo
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao resetar cache do processo: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao resetar cache do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )


@router.delete("/cache/documento/{documento_formatado}")
async def reset_cache_documento(documento_formatado: str):
    """
    Reseta o cache de um documento específico.

    Args:
        documento_formatado (str): Número do documento formatado

    Returns:
        dict: Resultado da operação
    """
    try:
        if not await cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Remove a chave específica do documento
        cache_key = gerar_chave_documento(documento_formatado)
        deleted = await cache.delete(cache_key)

        logger.info(f"Cache do documento {documento_formatado} resetado")

        return {
            "status": "ok",
            "message": f"Cache do documento {documento_formatado} resetado com sucesso",
            "deleted": deleted,
            "documento_formatado": documento_formatado
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao resetar cache do documento: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao resetar cache do documento",
                details={"error": str(e), "documento_formatado": documento_formatado}
            ).dict()
        )


@router.get("/cache/keys")
async def list_cache_keys(pattern: str = "*", limit: int = 100):
    """
    Lista as chaves do cache que correspondem ao padrão.

    Args:
        pattern (str): Padrão para filtrar chaves (padrão: "*" - todas)
        limit (int): Número máximo de chaves a retornar (padrão: 100)

    Returns:
        dict: Lista de chaves encontradas
    """
    try:
        if not await cache.is_available():
            raise HTTPException(
                status_code=503,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Redis não está disponível",
                    details={}
                ).dict()
            )

        # Lista as chaves usando SCAN
        keys = await cache.get_keys(pattern)
        total_keys = len(keys)

        # Limita o número de chaves retornadas
        limited_keys = keys[:limit]

        # Obtém TTL de cada chave
        keys_with_ttl = []
        for key in limited_keys:
            ttl = await cache.redis_client.ttl(key)
            keys_with_ttl.append({
                "key": key,
                "ttl": ttl if ttl > 0 else "sem expiração" if ttl == -1 else "expirado"
            })

        return {
            "status": "ok",
            "pattern": pattern,
            "total_keys": total_keys,
            "returned_keys": len(limited_keys),
            "limit": limit,
            "keys": keys_with_ttl
        }

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao listar chaves do cache: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao listar chaves do cache",
                details={"error": str(e)}
            ).dict()
        )


# --------------- Analytics Schemas ---------------

class LoginDiaItem(BaseModel):
    data: str
    logins_unicos: int
    total_logins: int

class LoginsOverTimeResponse(BaseModel):
    periodo: str
    items: list[LoginDiaItem]
    total_usuarios_unicos: int

class UsuarioAtivoItem(BaseModel):
    usuario_sei: str
    orgao: Optional[str] = None
    total_atividades: int
    processos_visualizados: int
    ultima_atividade: Optional[str] = None
    primeiro_acesso: Optional[str] = None

class UsuariosAtivosResponse(BaseModel):
    items: list[UsuarioAtivoItem]
    total: int
    page: int
    page_size: int

class ProcessoVisualizadoItem(BaseModel):
    numero_processo: str
    total_visualizacoes: int
    usuarios_distintos: int
    ultima_visualizacao: Optional[str] = None

class ProcessosVisualizadosResponse(BaseModel):
    items: list[ProcessoVisualizadoItem]
    total: int
    page: int
    page_size: int

class AcaoPorTipoItem(BaseModel):
    tipo_atividade: str
    total: int
    usuarios_distintos: int

class AcoesPorTipoResponse(BaseModel):
    periodo: str
    items: list[AcaoPorTipoItem]

class ResumoAnaliticoResponse(BaseModel):
    periodo: str
    total_usuarios_unicos: int
    total_logins: int
    total_visualizacoes_processo: int
    total_acoes: int
    usuario_mais_ativo: Optional[str] = None
    processo_mais_visto: Optional[str] = None


# --------------- Analytics helpers ---------------

def _periodo_to_date(periodo: str) -> datetime:
    """Converte string de periodo para data de inicio."""
    days_map = {"7d": 7, "30d": 30, "90d": 90, "365d": 365}
    days = days_map.get(periodo, 30)
    return datetime.now(timezone.utc) - timedelta(days=days)


# --------------- Analytics endpoints ---------------

@router.get("/analytics/resumo", dependencies=[Depends(require_admin)])
async def analytics_resumo(
    usuario_sei: str = Query(...),
    periodo: str = Query("30d"),
    db: AsyncSession = Depends(get_db),
):
    cache_key = f"analytics:resumo:{periodo}"
    cached = await cache.get(cache_key)
    if cached:
        return orjson.loads(cached)

    desde = _periodo_to_date(periodo)

    # Total unique users
    r1 = await db.execute(
        select(func.count(distinct(RegistroAtividade.usuario_sei))).where(
            RegistroAtividade.criado_em >= desde,
            RegistroAtividade.deletado_em.is_(None),
        )
    )
    total_usuarios = r1.scalar() or 0

    # Total logins
    r2 = await db.execute(
        select(func.count()).where(
            RegistroAtividade.tipo_atividade == "login",
            RegistroAtividade.criado_em >= desde,
            RegistroAtividade.deletado_em.is_(None),
        )
    )
    total_logins = r2.scalar() or 0

    # Total process views (from historico_pesquisas)
    r3 = await db.execute(
        select(func.count()).where(
            HistoricoPesquisa.criado_em >= desde,
            HistoricoPesquisa.deletado_em.is_(None),
        )
    )
    total_views = r3.scalar() or 0

    # Total actions
    r4 = await db.execute(
        select(func.count()).where(
            RegistroAtividade.criado_em >= desde,
            RegistroAtividade.deletado_em.is_(None),
        )
    )
    total_acoes = r4.scalar() or 0

    # Most active user
    r5 = await db.execute(
        select(RegistroAtividade.usuario_sei, func.count().label("cnt"))
        .where(
            RegistroAtividade.criado_em >= desde,
            RegistroAtividade.deletado_em.is_(None),
        )
        .group_by(RegistroAtividade.usuario_sei)
        .order_by(func.count().desc())
        .limit(1)
    )
    row5 = r5.first()
    usuario_mais_ativo = row5[0] if row5 else None

    # Most viewed process
    r6 = await db.execute(
        select(HistoricoPesquisa.numero_processo, func.count().label("cnt"))
        .where(
            HistoricoPesquisa.criado_em >= desde,
            HistoricoPesquisa.deletado_em.is_(None),
        )
        .group_by(HistoricoPesquisa.numero_processo)
        .order_by(func.count().desc())
        .limit(1)
    )
    row6 = r6.first()
    processo_mais_visto = row6[0] if row6 else None

    result = ResumoAnaliticoResponse(
        periodo=periodo,
        total_usuarios_unicos=total_usuarios,
        total_logins=total_logins,
        total_visualizacoes_processo=total_views,
        total_acoes=total_acoes,
        usuario_mais_ativo=usuario_mais_ativo,
        processo_mais_visto=processo_mais_visto,
    ).model_dump()

    await cache.set(cache_key, orjson.dumps(result).decode(), ttl=300)
    return result


@router.get("/analytics/logins-over-time", dependencies=[Depends(require_admin)])
async def analytics_logins_over_time(
    usuario_sei: str = Query(...),
    periodo: str = Query("30d"),
    db: AsyncSession = Depends(get_db),
):
    cache_key = f"analytics:logins:{periodo}"
    cached = await cache.get(cache_key)
    if cached:
        return orjson.loads(cached)

    desde = _periodo_to_date(periodo)

    stmt = (
        select(
            func.date(RegistroAtividade.criado_em).label("dia"),
            func.count(distinct(RegistroAtividade.usuario_sei)).label("unicos"),
            func.count().label("total"),
        )
        .where(
            RegistroAtividade.tipo_atividade == "login",
            RegistroAtividade.criado_em >= desde,
            RegistroAtividade.deletado_em.is_(None),
        )
        .group_by(func.date(RegistroAtividade.criado_em))
        .order_by(func.date(RegistroAtividade.criado_em))
    )
    rows = (await db.execute(stmt)).all()

    items = [LoginDiaItem(data=str(r.dia), logins_unicos=r.unicos, total_logins=r.total) for r in rows]
    total_unicos_stmt = (
        select(func.count(distinct(RegistroAtividade.usuario_sei)))
        .where(
            RegistroAtividade.tipo_atividade == "login",
            RegistroAtividade.criado_em >= desde,
            RegistroAtividade.deletado_em.is_(None),
        )
    )
    total_unicos = (await db.execute(total_unicos_stmt)).scalar() or 0

    result = LoginsOverTimeResponse(periodo=periodo, items=items, total_usuarios_unicos=total_unicos).model_dump()
    await cache.set(cache_key, orjson.dumps(result).decode(), ttl=300)
    return result


@router.get("/analytics/usuarios-ativos", dependencies=[Depends(require_admin)])
async def analytics_usuarios_ativos(
    usuario_sei: str = Query(...),
    periodo: str = Query("30d"),
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    desde = _periodo_to_date(periodo)

    # Subquery: activity counts per user
    ativ_sq = (
        select(
            RegistroAtividade.usuario_sei,
            func.count().label("total_atividades"),
            func.max(RegistroAtividade.criado_em).label("ultima_atividade"),
            func.min(RegistroAtividade.criado_em).label("primeiro_acesso"),
        )
        .where(
            RegistroAtividade.criado_em >= desde,
            RegistroAtividade.deletado_em.is_(None),
        )
        .group_by(RegistroAtividade.usuario_sei)
        .subquery()
    )

    # Subquery: process view counts per user from historico
    hist_sq = (
        select(
            HistoricoPesquisa.usuario,
            func.count(distinct(HistoricoPesquisa.numero_processo)).label("processos_visualizados"),
        )
        .where(
            HistoricoPesquisa.criado_em >= desde,
            HistoricoPesquisa.deletado_em.is_(None),
        )
        .group_by(HistoricoPesquisa.usuario)
        .subquery()
    )

    # Main query joining credenciais with activity data
    base = (
        select(
            CredencialUsuario.usuario_sei,
            CredencialUsuario.orgao,
            func.coalesce(ativ_sq.c.total_atividades, 0).label("total_atividades"),
            func.coalesce(hist_sq.c.processos_visualizados, 0).label("processos_visualizados"),
            ativ_sq.c.ultima_atividade,
            ativ_sq.c.primeiro_acesso,
        )
        .outerjoin(ativ_sq, CredencialUsuario.usuario_sei == ativ_sq.c.usuario_sei)
        .outerjoin(hist_sq, CredencialUsuario.usuario_sei == hist_sq.c.usuario)
        .where(CredencialUsuario.deletado_em.is_(None))
    )

    if search:
        base = base.where(
            CredencialUsuario.usuario_sei.ilike(f"%{search}%") |
            CredencialUsuario.orgao.ilike(f"%{search}%")
        )

    # Get distinct usuario_sei (credentials can have duplicates)
    base = base.group_by(
        CredencialUsuario.usuario_sei,
        CredencialUsuario.orgao,
        ativ_sq.c.total_atividades,
        hist_sq.c.processos_visualizados,
        ativ_sq.c.ultima_atividade,
        ativ_sq.c.primeiro_acesso,
    )

    # Count total
    count_stmt = select(func.count()).select_from(base.subquery())
    total = (await db.execute(count_stmt)).scalar() or 0

    # Paginate
    stmt = base.order_by(ativ_sq.c.ultima_atividade.desc().nullslast()).offset((page - 1) * page_size).limit(page_size)
    rows = (await db.execute(stmt)).all()

    items = [
        UsuarioAtivoItem(
            usuario_sei=r.usuario_sei,
            orgao=r.orgao,
            total_atividades=r.total_atividades,
            processos_visualizados=r.processos_visualizados,
            ultima_atividade=r.ultima_atividade.isoformat() if r.ultima_atividade else None,
            primeiro_acesso=r.primeiro_acesso.isoformat() if r.primeiro_acesso else None,
        )
        for r in rows
    ]

    return UsuariosAtivosResponse(items=items, total=total, page=page, page_size=page_size).model_dump()


@router.get("/analytics/processos-visualizados", dependencies=[Depends(require_admin)])
async def analytics_processos_visualizados(
    usuario_sei: str = Query(...),
    periodo: str = Query("30d"),
    filtro_usuario: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    desde = _periodo_to_date(periodo)

    base_where = [
        HistoricoPesquisa.criado_em >= desde,
        HistoricoPesquisa.deletado_em.is_(None),
    ]
    if filtro_usuario:
        base_where.append(HistoricoPesquisa.usuario == filtro_usuario)

    # Count total distinct processes
    count_stmt = select(func.count(distinct(HistoricoPesquisa.numero_processo))).where(*base_where)
    total = (await db.execute(count_stmt)).scalar() or 0

    # Get paginated results
    stmt = (
        select(
            HistoricoPesquisa.numero_processo,
            func.count().label("total_visualizacoes"),
            func.count(distinct(HistoricoPesquisa.usuario)).label("usuarios_distintos"),
            func.max(HistoricoPesquisa.criado_em).label("ultima_visualizacao"),
        )
        .where(*base_where)
        .group_by(HistoricoPesquisa.numero_processo)
        .order_by(func.count().desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    rows = (await db.execute(stmt)).all()

    items = [
        ProcessoVisualizadoItem(
            numero_processo=r.numero_processo,
            total_visualizacoes=r.total_visualizacoes,
            usuarios_distintos=r.usuarios_distintos,
            ultima_visualizacao=r.ultima_visualizacao.isoformat() if r.ultima_visualizacao else None,
        )
        for r in rows
    ]

    return ProcessosVisualizadosResponse(items=items, total=total, page=page, page_size=page_size).model_dump()


@router.get("/analytics/acoes-por-tipo", dependencies=[Depends(require_admin)])
async def analytics_acoes_por_tipo(
    usuario_sei: str = Query(...),
    periodo: str = Query("30d"),
    filtro_usuario: Optional[str] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    cache_key = f"analytics:acoes:{periodo}:{filtro_usuario or 'all'}"
    cached = await cache.get(cache_key)
    if cached:
        return orjson.loads(cached)

    desde = _periodo_to_date(periodo)

    base_where = [
        RegistroAtividade.criado_em >= desde,
        RegistroAtividade.deletado_em.is_(None),
    ]
    if filtro_usuario:
        base_where.append(RegistroAtividade.usuario_sei == filtro_usuario)

    stmt = (
        select(
            RegistroAtividade.tipo_atividade,
            func.count().label("total"),
            func.count(distinct(RegistroAtividade.usuario_sei)).label("usuarios_distintos"),
        )
        .where(*base_where)
        .group_by(RegistroAtividade.tipo_atividade)
        .order_by(func.count().desc())
    )
    rows = (await db.execute(stmt)).all()

    items = [
        AcaoPorTipoItem(
            tipo_atividade=r.tipo_atividade,
            total=r.total,
            usuarios_distintos=r.usuarios_distintos,
        )
        for r in rows
    ]

    result = AcoesPorTipoResponse(periodo=periodo, items=items).model_dump()
    await cache.set(cache_key, orjson.dumps(result).decode(), ttl=300)
    return result
