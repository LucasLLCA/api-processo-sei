import asyncio
import json
import math
import logging
from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel
from starlette.responses import StreamingResponse
from ..sei import (
    login, contar_andamentos, buscar_pagina_andamentos,
    listar_documentos, listar_documentos_parcial,
    consultar_procedimento, verificar_saude, assinar_documento,
    consultar_documento,
)
from ..cache import cache
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


def _build_andamentos_result(andamentos: list, total_itens: int, numero_processo: str) -> dict:
    """Build a standardized andamentos response dict."""
    return {
        "Info": {
            "Pagina": 1,
            "TotalPaginas": 1,
            "QuantidadeItens": len(andamentos),
            "TotalItens": total_itens,
            "NumeroProcesso": numero_processo,
            "Parcial": len(andamentos) < total_itens,
        },
        "Andamentos": andamentos,
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


async def _safe_cache_update(cache_key: str, new_result: dict, ttl: int):
    """Only update cache if new data has more items (prevents regression from concurrent writes)."""
    current = await cache.get(cache_key)
    if current and len(current.get("Andamentos", [])) >= len(new_result.get("Andamentos", [])):
        return
    await cache.set(cache_key, new_result, ttl=ttl)


class LoginRequest(BaseModel):
    usuario: str
    senha: str
    orgao: str


@router.post("/login")
async def sei_login(body: LoginRequest):
    """
    Proxy para login na API SEI.
    Retorna a resposta bruta da API SEI (Token, Login, Unidades).
    """
    logger.info(f"POST /sei/login INCOMING — user={body.usuario} orgao={body.orgao} senha_len={len(body.senha)}")
    try:
        result = await login(body.usuario, body.senha, body.orgao)
        logger.info(f"POST /sei/login OK for user={body.usuario} orgao={body.orgao} — keys={list(result.keys()) if isinstance(result, dict) else type(result).__name__}")
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
    parcial: bool = Query(False),
    extrair_documentos: bool = Query(False),
):
    """
    Proxy para buscar andamentos de um processo com cache incremental.

    Always queries metadata (quantidade=0) to check current TotalItens.
    Caches both partial and full results for 1 month.
    Invalidates when TotalItens changes, reusing existing cached data
    and only fetching the new/missing andamentos.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    cache_key = f"proxy:andamentos:{numero_processo}:{id_unidade}"

    # Step 1: Always query metadata to get current TotalItens
    current_total = await contar_andamentos(x_sei_token, numero_processo, id_unidade)

    if current_total == 0:
        resultado = _build_andamentos_result([], 0, numero_processo)
        if extrair_documentos:
            resultado["DocumentosExtraidos"] = _extract_documents_from_andamentos([])
        await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
        return resultado

    total_pages = math.ceil(current_total / ANDAMENTOS_PAGE_SIZE)

    def _add_doc_extraction(resultado: dict, andamentos_for_extraction: list = None):
        """Add DocumentosExtraidos to result if flag is set."""
        if extrair_documentos:
            source = andamentos_for_extraction or resultado.get("Andamentos", [])
            resultado["DocumentosExtraidos"] = _extract_documents_from_andamentos(source)

    async def _fetch_last_page_for_extraction(andamentos: list) -> list:
        """Fetch last page and combine with existing andamentos for doc extraction."""
        if not extrair_documentos or total_pages <= 2:
            return andamentos
        try:
            last_page = await buscar_pagina_andamentos(
                x_sei_token, numero_processo, id_unidade, total_pages, ANDAMENTOS_PAGE_SIZE,
            )
            return andamentos + last_page
        except Exception:
            logger.warning(f"Failed to fetch last page for doc extraction: {numero_processo}")
            return andamentos

    # Step 2: Check cache
    cached = await cache.get(cache_key)

    if cached:
        cached_total = cached["Info"]["TotalItens"]
        cached_count = len(cached.get("Andamentos", []))
        cached_is_full = not cached["Info"].get("Parcial", True)

        if cached_total == current_total:
            # TotalItens unchanged
            if cached_is_full:
                _add_doc_extraction(cached)
                return cached

            # Partial cache, same total — serve cached, background fill remaining
            if parcial:
                asyncio.create_task(_background_fill_andamentos(
                    x_sei_token, numero_processo, id_unidade, cache_key,
                    cached["Andamentos"], cached_count, current_total,
                ))
                extraction_source = await _fetch_last_page_for_extraction(cached["Andamentos"])
                _add_doc_extraction(cached, extraction_source)
                return cached

            # Caller wants full — fetch remaining synchronously
            cached_complete_pages = cached_count // ANDAMENTOS_PAGE_SIZE
            remaining = await _fetch_andamentos_pages(
                x_sei_token, numero_processo, id_unidade,
                cached_complete_pages + 1, total_pages,
            )
            all_andamentos = cached["Andamentos"][:cached_complete_pages * ANDAMENTOS_PAGE_SIZE] + remaining
            resultado = _build_andamentos_result(all_andamentos, current_total, numero_processo)
            _add_doc_extraction(resultado)
            await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
            return resultado

        if current_total > cached_total:
            # New andamentos added — reuse cached, fetch only new pages
            logger.info(
                f"TotalItens changed: {cached_total} → {current_total} for {numero_processo}. "
                f"Reusing {cached_count} cached items."
            )
            cached_complete_pages = cached_count // ANDAMENTOS_PAGE_SIZE
            reusable = cached["Andamentos"][:cached_complete_pages * ANDAMENTOS_PAGE_SIZE]

            if parcial:
                resultado = _build_andamentos_result(reusable, current_total, numero_processo)
                await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
                asyncio.create_task(_background_fill_andamentos(
                    x_sei_token, numero_processo, id_unidade, cache_key,
                    reusable, len(reusable), current_total,
                ))
                extraction_source = await _fetch_last_page_for_extraction(reusable)
                _add_doc_extraction(resultado, extraction_source)
                return resultado

            # Full request — fetch all missing synchronously
            remaining = await _fetch_andamentos_pages(
                x_sei_token, numero_processo, id_unidade,
                cached_complete_pages + 1, total_pages,
            )
            all_andamentos = reusable + remaining
            resultado = _build_andamentos_result(all_andamentos, current_total, numero_processo)
            _add_doc_extraction(resultado)
            await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
            return resultado

        # TotalItens decreased (unusual) — invalidate and fall through to fresh fetch
        logger.warning(
            f"TotalItens decreased: {cached_total} → {current_total} for {numero_processo}. "
            f"Invalidating cache."
        )
        await cache.delete(cache_key)

    # Step 3: No cache (or invalidated) — fresh fetch
    if parcial:
        if current_total <= ANDAMENTOS_PAGE_SIZE:
            # Single fetch gets everything
            andamentos = await buscar_pagina_andamentos(
                x_sei_token, numero_processo, id_unidade, 1, ANDAMENTOS_PAGE_SIZE,
            )
            resultado = _build_andamentos_result(andamentos, current_total, numero_processo)
            _add_doc_extraction(resultado)
            await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
            return resultado

        if current_total <= ANDAMENTOS_PAGE_SIZE * 2:
            # 1-2 pages total: fetch page 1 (+ last page for extraction if needed)
            if extrair_documentos and total_pages == 2:
                page1, last_page = await asyncio.gather(
                    buscar_pagina_andamentos(x_sei_token, numero_processo, id_unidade, 1, ANDAMENTOS_PAGE_SIZE),
                    buscar_pagina_andamentos(x_sei_token, numero_processo, id_unidade, total_pages, ANDAMENTOS_PAGE_SIZE),
                )
                andamentos = page1
                resultado = _build_andamentos_result(andamentos, current_total, numero_processo)
                _add_doc_extraction(resultado, page1 + last_page)
            else:
                andamentos = await buscar_pagina_andamentos(
                    x_sei_token, numero_processo, id_unidade, 1, ANDAMENTOS_PAGE_SIZE,
                )
                resultado = _build_andamentos_result(andamentos, current_total, numero_processo)
                _add_doc_extraction(resultado)
            await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
            asyncio.create_task(_background_fill_andamentos(
                x_sei_token, numero_processo, id_unidade, cache_key,
                andamentos, len(andamentos), current_total,
            ))
            return resultado

        # > 80 items: pages 1+2 for timeline, + last page for doc extraction
        if extrair_documentos:
            page1, page2, last_page = await asyncio.gather(
                buscar_pagina_andamentos(x_sei_token, numero_processo, id_unidade, 1, ANDAMENTOS_PAGE_SIZE),
                buscar_pagina_andamentos(x_sei_token, numero_processo, id_unidade, 2, ANDAMENTOS_PAGE_SIZE),
                buscar_pagina_andamentos(x_sei_token, numero_processo, id_unidade, total_pages, ANDAMENTOS_PAGE_SIZE),
            )
            andamentos = page1 + page2
            resultado = _build_andamentos_result(andamentos, current_total, numero_processo)
            _add_doc_extraction(resultado, page1 + page2 + last_page)
        else:
            page1, page2 = await asyncio.gather(
                buscar_pagina_andamentos(x_sei_token, numero_processo, id_unidade, 1, ANDAMENTOS_PAGE_SIZE),
                buscar_pagina_andamentos(x_sei_token, numero_processo, id_unidade, 2, ANDAMENTOS_PAGE_SIZE),
            )
            andamentos = page1 + page2
            resultado = _build_andamentos_result(andamentos, current_total, numero_processo)
            _add_doc_extraction(resultado)
        await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
        asyncio.create_task(_background_fill_andamentos(
            x_sei_token, numero_processo, id_unidade, cache_key,
            andamentos, len(andamentos), current_total,
        ))
        return resultado

    # parcial=false, no cache — fetch all pages
    all_andamentos = await _fetch_andamentos_pages(
        x_sei_token, numero_processo, id_unidade, 1, total_pages,
    )
    resultado = _build_andamentos_result(all_andamentos, current_total, numero_processo)
    _add_doc_extraction(resultado)
    await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
    return resultado


async def _background_fill_andamentos(
    token, numero_processo, id_unidade, cache_key,
    existing_andamentos, existing_count, total_itens,
):
    """Background task: fetch remaining andamentos incrementally and update cache."""
    try:
        cached_complete_pages = existing_count // ANDAMENTOS_PAGE_SIZE
        total_pages = math.ceil(total_itens / ANDAMENTOS_PAGE_SIZE)
        start_page = cached_complete_pages + 1

        if start_page > total_pages:
            return

        logger.info(
            f"Background fill starting: {numero_processo} "
            f"pages {start_page}-{total_pages} ({existing_count}/{total_itens} cached)"
        )

        remaining = await _fetch_andamentos_pages(
            token, numero_processo, id_unidade, start_page, total_pages,
        )

        if remaining:
            reusable = existing_andamentos[:cached_complete_pages * ANDAMENTOS_PAGE_SIZE]
            all_andamentos = reusable + remaining
            resultado = _build_andamentos_result(all_andamentos, total_itens, numero_processo)
            await _safe_cache_update(cache_key, resultado, CACHE_TTL_ANDAMENTOS)
            logger.info(
                f"Background fill complete: {numero_processo} "
                f"({len(all_andamentos)} items cached)"
            )
    except Exception as e:
        logger.error(f"Background fill failed: {numero_processo} — {e}")


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
        return {
            "UnidadesProcedimentoAberto": cached["UnidadesProcedimentoAberto"],
            "LinkAcesso": cached.get("LinkAcesso"),
        }

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


def _sse_event(data: dict) -> str:
    """Formata um evento SSE."""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


@router.get("/andamentos-stream/{numero_processo}")
async def sei_andamentos_stream(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
):
    """
    SSE endpoint for andamentos with progress events.
    Leverages cached data: if full cache exists returns immediately,
    otherwise streams only the remaining pages with progress.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    cache_key = f"proxy:andamentos:{numero_processo}:{id_unidade}"

    # Check cache — if full data is already cached, return immediately
    cached = await cache.get(cache_key)
    if cached and not cached["Info"].get("Parcial", True):
        logger.info(f"[andamentos-stream] Full cache hit for processo={numero_processo}")

        async def cached_generator():
            yield _sse_event({"type": "done", "content": cached})

        return StreamingResponse(
            cached_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # Determine current total and what we already have
    current_total = await contar_andamentos(x_sei_token, numero_processo, id_unidade)

    existing = cached["Andamentos"] if cached else []
    existing_count = len(existing)

    # If total changed, only keep complete pages
    if cached and cached["Info"]["TotalItens"] != current_total:
        complete_pages = existing_count // ANDAMENTOS_PAGE_SIZE
        existing = existing[:complete_pages * ANDAMENTOS_PAGE_SIZE]
        existing_count = len(existing)

    async def stream_generator():
        try:
            if current_total == 0:
                resultado = _build_andamentos_result([], 0, numero_processo)
                yield _sse_event({"type": "done", "content": resultado})
                return

            if existing_count >= current_total:
                resultado = _build_andamentos_result(existing, current_total, numero_processo)
                yield _sse_event({"type": "done", "content": resultado})
                return

            # Yield initial progress
            yield _sse_event({"type": "progress", "content": {"loaded": existing_count, "total": current_total}})

            # Fetch remaining pages in batches, streaming progress
            total_pages = math.ceil(current_total / ANDAMENTOS_PAGE_SIZE)
            cached_pages = existing_count // ANDAMENTOS_PAGE_SIZE
            pages_to_fetch = list(range(cached_pages + 1, total_pages + 1))

            all_andamentos = list(existing)

            for i in range(0, len(pages_to_fetch), ANDAMENTOS_BATCH_SIZE):
                batch = pages_to_fetch[i:i + ANDAMENTOS_BATCH_SIZE]
                tasks = [
                    buscar_pagina_andamentos(x_sei_token, numero_processo, id_unidade, p, ANDAMENTOS_PAGE_SIZE)
                    for p in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for result in results:
                    if isinstance(result, Exception):
                        continue
                    all_andamentos.extend(result)

                yield _sse_event({"type": "progress", "content": {"loaded": len(all_andamentos), "total": current_total}})

            resultado = _build_andamentos_result(all_andamentos, current_total, numero_processo)
            await _safe_cache_update(cache_key, resultado, CACHE_TTL_ANDAMENTOS)
            yield _sse_event({"type": "done", "content": resultado})

        except Exception as e:
            logger.error(f"[andamentos-stream] Erro: {str(e)}", exc_info=True)
            yield _sse_event({"type": "error", "content": f"Erro ao buscar andamentos: {str(e)}"})

    return StreamingResponse(
        stream_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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
