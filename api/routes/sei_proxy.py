import asyncio
import math
import logging
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from ..sei import (
    login, contar_andamentos, buscar_pagina_andamentos,
    listar_documentos, listar_documentos_parcial,
    consultar_procedimento, verificar_saude, assinar_documento,
    consultar_documento,
)
from ..cache import cache
from ..database import get_db
from ..models.configuracao_horas import ConfiguracaoHorasAndamento
from ..models.credencial_usuario import CredencialUsuario
from ..normalization import normalizar_numero_processo

logger = logging.getLogger(__name__)

router = APIRouter()

# Cache TTLs for proxy endpoints
CACHE_TTL_ANDAMENTOS = 2_592_000   # 1 month
CACHE_TTL_UNIDADES = 86400         # 1 day

# Andamentos page size (matches listar_tarefa_parcial FETCH_SIZE)
ANDAMENTOS_PAGE_SIZE = 40
ANDAMENTOS_BATCH_SIZE = 5  # parallel pages per batch

# Task types that reference documents
DOC_TASK_PREFIXES = (
    "GERACAO-DOCUMENTO",
    "ASSINATURA-DOCUMENTO",
    "DOCUMENTO-INCLUIDO-EM-BLOCO",
    "DOCUMENTO-RETIRADO-DO-BLOCO",
)


def _extract_documents_from_andamentos(andamentos: list) -> dict:
    """
    Extract unique DocumentoFormatado IDs from andamentos with document-related tasks.
    Filters by Tarefa type, then takes the first Atributo's Valor from each match.
    Returns dict with primeiro, ultimo, and all unique doc IDs in order.
    """
    seen = set()
    ordered = []
    for a in andamentos:
        tarefa = a.get("Tarefa", "")
        if not any(tarefa.startswith(p) for p in DOC_TASK_PREFIXES):
            continue
        attrs = a.get("Atributos") or []
        if attrs:
            val = attrs[0].get("Valor", "")
            if val and val not in seen:
                seen.add(val)
                ordered.append(val)
    return {
        "todos": ordered,
        "primeiro": ordered[0] if ordered else None,
        "ultimo": ordered[-1] if ordered else None,
        "total": len(ordered),
    }


def _deduplicate_andamentos(andamentos: list) -> list:
    """Remove duplicate andamentos by IdAndamento, keeping first occurrence."""
    seen = set()
    result = []
    for a in andamentos:
        aid = a.get("IdAndamento")
        if aid and aid in seen:
            continue
        if aid:
            seen.add(aid)
        result.append(a)
    return result


def _build_andamentos_result(andamentos: list, total_itens: int, numero_processo: str) -> dict:
    """Build a standardized andamentos response dict."""
    deduped = _deduplicate_andamentos(andamentos)
    return {
        "Info": {
            "Pagina": 1,
            "TotalPaginas": 1,
            "QuantidadeItens": len(deduped),
            "TotalItens": total_itens,
            "NumeroProcesso": numero_processo,
            "Parcial": len(deduped) < total_itens,
        },
        "Andamentos": deduped,
    }


async def _fetch_andamentos_pages(token, protocolo, id_unidade, start_page, end_page):
    """Fetch andamentos for pages [start_page, end_page] in parallel batches."""
    pages = list(range(start_page, end_page + 1))
    all_andamentos = []

    for i in range(0, len(pages), ANDAMENTOS_BATCH_SIZE):
        batch = pages[i:i + ANDAMENTOS_BATCH_SIZE]
        tasks = [
            buscar_pagina_andamentos(token, protocolo, id_unidade, p, ANDAMENTOS_PAGE_SIZE)
            for p in batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                continue
            all_andamentos.extend(result)

    return all_andamentos


class LoginRequest(BaseModel):
    usuario: str
    senha: str
    orgao: str


@router.post("/login")
async def sei_login(body: LoginRequest, db: AsyncSession = Depends(get_db)):
    """
    Proxy para login na API SEI.
    Retorna a resposta bruta da API SEI (Token, Login, Unidades)
    enriquecida com papel_global e id_pessoa da tabela credencial_usuarios.
    """
    logger.info(f"POST /sei/login INCOMING — user={body.usuario} orgao={body.orgao} senha_len={len(body.senha)}")
    try:
        result = await login(body.usuario, body.senha, body.orgao)
        logger.info(f"POST /sei/login OK for user={body.usuario} orgao={body.orgao} — keys={list(result.keys()) if isinstance(result, dict) else type(result).__name__}")

        # Enrich with papel_global and id_pessoa from credencial_usuarios
        if isinstance(result, dict):
            try:
                cred = None
                # Try lookup by IdPessoa first
                id_pessoa_sei = result.get("Login", {}).get("IdPessoa")
                if id_pessoa_sei:
                    cred_result = await db.execute(
                        select(CredencialUsuario).where(
                            CredencialUsuario.id_pessoa == int(id_pessoa_sei),
                            CredencialUsuario.deletado_em.is_(None),
                        )
                    )
                    cred = cred_result.scalar_one_or_none()

                # Fallback: lookup by usuario_sei + orgao
                if not cred:
                    cred_result = await db.execute(
                        select(CredencialUsuario).where(
                            CredencialUsuario.usuario_sei == body.usuario,
                            CredencialUsuario.orgao == body.orgao,
                            CredencialUsuario.deletado_em.is_(None),
                        )
                    )
                    cred = cred_result.scalar_one_or_none()

                if cred:
                    result["papel_global"] = cred.papel_global
                    result["id_pessoa"] = cred.id_pessoa
                    logger.info(f"Enriched login with papel_global={cred.papel_global} id_pessoa={cred.id_pessoa}")
                else:
                    logger.info(f"No credencial found for user={body.usuario} orgao={body.orgao} — papel_global not set")
            except Exception as e:
                logger.warning(f"Could not enrich login with papel_global: {e}")

        return result
    except HTTPException as he:
        logger.error(f"POST /sei/login HTTPException for user={body.usuario} orgao={body.orgao} — status={he.status_code} detail={he.detail}")
        raise
    except Exception as e:
        logger.exception(f"POST /sei/login 500 for user={body.usuario} orgao={body.orgao} — {type(e).__name__}: {e}")
        raise


@router.get("/andamentos/{numero_processo}")
async def sei_andamentos(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
    quantidade: int = Query(None, description="Items per page (overrides default 40). Used for delta fetches."),
    pagina: int = Query(None, description="Page number to fetch. Used for delta fetches."),
):
    """
    Proxy para buscar andamentos de um processo.

    When quantidade+pagina are provided: simple single-page fetch (for delta sync).
    Otherwise: full fetch with cache.
    """
    numero_processo = normalizar_numero_processo(numero_processo)

    # ── Delta fetch mode: simple single-page request, no caching ──
    if quantidade is not None and pagina is not None:
        if quantidade <= 0:
            return _build_andamentos_result([], 0, numero_processo)

        andamentos = await buscar_pagina_andamentos(
            x_sei_token, numero_processo, id_unidade, pagina, quantidade,
        )
        current_total = await contar_andamentos(x_sei_token, numero_processo, id_unidade)
        return _build_andamentos_result(andamentos, current_total, numero_processo)

    # ── Full fetch mode with cache ──
    cache_key = f"proxy:andamentos:{numero_processo}:{id_unidade}"

    current_total = await contar_andamentos(x_sei_token, numero_processo, id_unidade)

    if current_total == 0:
        resultado = _build_andamentos_result([], 0, numero_processo)
        await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
        return resultado

    total_pages = math.ceil(current_total / ANDAMENTOS_PAGE_SIZE)

    # Check cache
    cached = await cache.get(cache_key)

    if cached:
        cached_total = cached["Info"]["TotalItens"]
        cached_count = len(cached.get("Andamentos", []))
        cached_is_full = not cached["Info"].get("Parcial", True)
        logger.info(
            f"GET /sei/andamentos/{numero_processo} — cache HIT "
            f"(cached={cached_count} total={cached_total} full={cached_is_full})"
        )

        if cached_total == current_total and cached_is_full:
            return cached

        if current_total > cached_total or (cached_total == current_total and not cached_is_full):
            # Reuse cached pages, fetch only missing
            cached_complete_pages = cached_count // ANDAMENTOS_PAGE_SIZE
            reusable = cached["Andamentos"][:cached_complete_pages * ANDAMENTOS_PAGE_SIZE]
            remaining = await _fetch_andamentos_pages(
                x_sei_token, numero_processo, id_unidade,
                cached_complete_pages + 1, total_pages,
            )
            all_andamentos = reusable + remaining
            resultado = _build_andamentos_result(all_andamentos, current_total, numero_processo)
            await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
            return resultado

        # TotalItens decreased — invalidate
        logger.warning(
            f"TotalItens decreased: {cached_total} → {current_total} for {numero_processo}. "
            f"Invalidating cache."
        )
        await cache.delete(cache_key)

    if not cached:
        logger.info(f"GET /sei/andamentos/{numero_processo} — cache MISS")

    # No cache — fetch all pages
    all_andamentos = await _fetch_andamentos_pages(
        x_sei_token, numero_processo, id_unidade, 1, total_pages,
    )
    resultado = _build_andamentos_result(all_andamentos, current_total, numero_processo)
    await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
    return resultado


@router.get("/andamentos-count/{numero_processo}")
async def sei_andamentos_count(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
):
    """
    Lightweight metadata-only request returning just TotalItens.
    Uses quantidade=0 internally — no andamento data is transferred.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    total = await contar_andamentos(x_sei_token, numero_processo, id_unidade)
    return {"total_itens": total}


@router.get("/unidades-abertas/{numero_processo}")
async def sei_unidades_abertas(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
):
    """
    Proxy para consultar unidades com processo aberto.
    Cache validated by TotalItens from andamentos metadata —
    if TotalItens hasn't changed, open units haven't changed either.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    cache_key = f"proxy:unidades:{numero_processo}:{id_unidade}"

    # Get current TotalItens to validate cache
    current_total = await contar_andamentos(x_sei_token, numero_processo, id_unidade)

    cached = await cache.get(cache_key)
    if cached and cached.get("_total_andamentos") == current_total:
        logger.info(f"GET /sei/unidades-abertas/{numero_processo} — cache HIT")
        return {
            "UnidadesProcedimentoAberto": cached["UnidadesProcedimentoAberto"],
            "LinkAcesso": cached.get("LinkAcesso"),
        }

    logger.info(
        f"GET /sei/unidades-abertas/{numero_processo} — cache MISS"
        f"{' (stale)' if cached else ''}"
    )
    data = await consultar_procedimento(x_sei_token, numero_processo, id_unidade)

    await cache.set(cache_key, {
        "UnidadesProcedimentoAberto": data.get("UnidadesProcedimentoAberto", []),
        "LinkAcesso": data.get("LinkAcesso"),
        "_total_andamentos": current_total,
    }, ttl=CACHE_TTL_ANDAMENTOS)

    return {
        "UnidadesProcedimentoAberto": data.get("UnidadesProcedimentoAberto", []),
        "LinkAcesso": data.get("LinkAcesso"),
    }


@router.get("/documentos/{numero_processo}")
async def sei_documentos(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
    parcial: bool = Query(False),
):
    """
    Proxy para buscar documentos de um processo.
    Cache validated by TotalItens from andamentos metadata —
    if TotalItens hasn't changed, documents haven't changed either.
    Cached for 1 month. Se parcial=true e sem cache, retorna primeira+última
    página imediatamente e dispara busca completa em background.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    cache_key = f"proxy:documentos:{numero_processo}:{id_unidade}"
    logger.info(
        f"GET /sei/documentos/{numero_processo} — "
        f"id_unidade={id_unidade} parcial={parcial}"
    )

    try:
        # Get current TotalItens to validate cache
        current_total = await contar_andamentos(x_sei_token, numero_processo, id_unidade)

        # Check cache with TotalItens validation
        cached = await cache.get(cache_key)
        if cached and cached.get("_total_andamentos") == current_total:
            logger.info(
                f"GET /sei/documentos/{numero_processo} — cache HIT "
                f"({cached.get('Info', {}).get('QuantidadeItens', '?')} docs)"
            )
            # Return without internal _total_andamentos field
            return {
                "Info": cached["Info"],
                "Documentos": cached["Documentos"],
            }

        logger.info(
            f"GET /sei/documentos/{numero_processo} — cache MISS"
            f"{' (stale)' if cached else ''}"
        )

        if parcial:
            documentos, total_itens, is_parcial = await listar_documentos_parcial(
                x_sei_token, numero_processo, id_unidade
            )

            resultado = {
                "Info": {
                    "Pagina": 1,
                    "TotalPaginas": 1,
                    "QuantidadeItens": len(documentos),
                    "TotalItens": total_itens,
                    "Parcial": is_parcial,
                },
                "Documentos": documentos,
            }

            logger.info(
                f"GET /sei/documentos/{numero_processo} OK — "
                f"parcial={is_parcial} returned={len(documentos)} total={total_itens}"
            )

            if not is_parcial:
                await cache.set(cache_key, {**resultado, "_total_andamentos": current_total}, ttl=CACHE_TTL_ANDAMENTOS)
            else:
                async def _background_full_fetch_docs():
                    try:
                        logger.info(f"Background full fetch starting: documentos processo={numero_processo}")
                        all_documentos = await listar_documentos(x_sei_token, numero_processo, id_unidade)
                        full_resultado = {
                            "Info": {
                                "Pagina": 1,
                                "TotalPaginas": 1,
                                "QuantidadeItens": len(all_documentos),
                                "TotalItens": len(all_documentos),
                                "Parcial": False,
                            },
                            "Documentos": all_documentos,
                            "_total_andamentos": current_total,
                        }
                        await cache.set(cache_key, full_resultado, ttl=CACHE_TTL_ANDAMENTOS)
                        logger.info(
                            f"Background full fetch completed: documentos processo={numero_processo} "
                            f"total={len(all_documentos)}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Background full fetch failed: documentos processo={numero_processo} — "
                            f"{type(e).__name__}: {e}"
                        )

                asyncio.create_task(_background_full_fetch_docs())

            return resultado

        # Full fetch (parcial=false)
        documentos = await listar_documentos(x_sei_token, numero_processo, id_unidade)

        resultado = {
            "Info": {
                "Pagina": 1,
                "TotalPaginas": 1,
                "QuantidadeItens": len(documentos),
                "TotalItens": len(documentos),
                "Parcial": False,
            },
            "Documentos": documentos,
        }

        logger.info(
            f"GET /sei/documentos/{numero_processo} OK — "
            f"full fetch returned={len(documentos)}"
        )
        await cache.set(cache_key, {**resultado, "_total_andamentos": current_total}, ttl=CACHE_TTL_ANDAMENTOS)
        return resultado

    except HTTPException as he:
        logger.error(
            f"GET /sei/documentos/{numero_processo} HTTPException — "
            f"status={he.status_code} detail={he.detail}"
        )
        raise
    except Exception as e:
        logger.exception(
            f"GET /sei/documentos/{numero_processo} 500 — "
            f"{type(e).__name__}: {e}"
        )
        raise


class AssinarDocumentoRequest(BaseModel):
    orgao: str
    cargo: str
    id_login: str
    senha: str
    id_usuario: str


@router.post("/documentos/{protocolo_documento}/assinar")
async def sei_assinar_documento(
    protocolo_documento: str,
    body: AssinarDocumentoRequest,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
):
    """
    Proxy para assinar um documento no SEI.
    """
    try:
        logger.info(
            f"POST /sei/documentos/{protocolo_documento}/assinar INCOMING "
            f"unidade={id_unidade} orgao={body.orgao} cargo={body.cargo} "
            f"id_login={body.id_login} id_usuario={body.id_usuario} "
            f"senha_len={len(body.senha)} token_len={len(x_sei_token)}"
        )
        result = await assinar_documento(
            x_sei_token, id_unidade, protocolo_documento,
            body.orgao, body.cargo, body.id_login, body.senha, body.id_usuario
        )
        logger.info(f"POST /sei/documentos/{protocolo_documento}/assinar OK unidade={id_unidade} result={result}")

        # Invalidate document caches so refetch picks up the new signature
        deleted = await cache.clear_pattern(f"proxy:documentos:*:{id_unidade}")
        logger.info(f"Cache documentos invalidado após assinatura: {deleted} chaves removidas")

        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"POST /sei/documentos/{protocolo_documento}/assinar 500 — {type(e).__name__}: {e}")
        raise


@router.delete("/cache/{numero_processo}")
async def sei_invalidar_cache(numero_processo: str):
    """
    Invalida todo o cache proxy de um processo específico.
    Remove andamentos, unidades e documentos cacheados.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    deleted = 0
    for pattern in [
        f"proxy:andamentos:{numero_processo}:*",
        f"proxy:unidades:{numero_processo}:*",
        f"proxy:documentos:{numero_processo}:*",
    ]:
        deleted += await cache.clear_pattern(pattern)

    logger.info(f"Cache proxy invalidado para processo {numero_processo}: {deleted} chaves removidas")

    return {
        "status": "ok",
        "message": f"Cache proxy do processo {numero_processo} invalidado",
        "keys_deleted": deleted,
    }


@router.get("/documento/{documento_formatado}")
async def sei_consultar_documento(
    documento_formatado: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
):
    """
    Proxy para consultar metadados de um documento específico no SEI.
    Retorna informações como Serie, Assinaturas, LinkAcesso, etc.
    """
    try:
        result = await consultar_documento(x_sei_token, id_unidade, documento_formatado)
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"GET /sei/documento/{documento_formatado} 500 — {type(e).__name__}: {e}")
        raise


@router.get("/health")
async def sei_health():
    """
    Verifica saúde da API SEI.
    """
    return await verificar_saude()


@router.get("/configuracao-horas")
async def get_configuracao_horas_public(
    orgao: str = Query(...),
    db: AsyncSession = Depends(get_db),
):
    """Public read-only endpoint for hour coefficients (used by productivity table)."""
    result = await db.execute(
        select(ConfiguracaoHorasAndamento).where(
            ConfiguracaoHorasAndamento.orgao == orgao
        ).order_by(ConfiguracaoHorasAndamento.grupo_key)
    )
    rows = result.scalars().all()
    return {r.grupo_key: r.horas for r in rows}
