import asyncio
import json
import logging
from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel
from starlette.responses import StreamingResponse
from ..sei import (
    login, listar_tarefa, listar_tarefa_parcial, listar_tarefa_stream,
    listar_documentos, listar_documentos_parcial,
    consultar_procedimento, verificar_saude, assinar_documento,
)
from ..cache import cache
from ..normalization import normalizar_numero_processo

logger = logging.getLogger(__name__)

router = APIRouter()

# Cache TTLs for proxy endpoints (1 day)
CACHE_TTL_ANDAMENTOS = 86400
CACHE_TTL_UNIDADES = 86400
CACHE_TTL_DOCUMENTOS = 86400


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
):
    """
    Proxy para buscar andamentos de um processo.
    Se parcial=true e sem cache, retorna primeiros+últimos andamentos imediatamente
    e dispara busca completa em background para preencher o cache.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    cache_key = f"proxy:andamentos:{numero_processo}:{id_unidade}"

    # Always check full cache first — return it regardless of parcial flag
    cached = await cache.get(cache_key)
    if cached:
        return cached

    if parcial:
        # Partial fetch: return first+last pages immediately
        andamentos, total_itens, is_parcial = await listar_tarefa_parcial(
            x_sei_token, numero_processo, id_unidade
        )

        resultado = {
            "Info": {
                "Pagina": 1,
                "TotalPaginas": 1,
                "QuantidadeItens": len(andamentos),
                "TotalItens": total_itens,
                "NumeroProcesso": numero_processo,
                "Parcial": is_parcial,
            },
            "Andamentos": andamentos,
        }

        if not is_parcial:
            # Small process — we got all data, cache it
            await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
        else:
            # Fire-and-forget: fetch ALL pages in background and cache
            async def _background_full_fetch():
                try:
                    logger.info(f"Background full fetch starting: andamentos processo={numero_processo}")
                    all_andamentos = await listar_tarefa(x_sei_token, numero_processo, id_unidade)
                    full_resultado = {
                        "Info": {
                            "Pagina": 1,
                            "TotalPaginas": 1,
                            "QuantidadeItens": len(all_andamentos),
                            "TotalItens": len(all_andamentos),
                            "NumeroProcesso": numero_processo,
                            "Parcial": False,
                        },
                        "Andamentos": all_andamentos,
                    }
                    await cache.set(cache_key, full_resultado, ttl=CACHE_TTL_ANDAMENTOS)
                    logger.info(
                        f"Background full fetch completed: andamentos processo={numero_processo} "
                        f"total={len(all_andamentos)}"
                    )
                except Exception as e:
                    logger.error(f"Background full fetch failed: andamentos processo={numero_processo} — {e}")

            asyncio.create_task(_background_full_fetch())

        return resultado

    # Full fetch (parcial=false)
    andamentos = await listar_tarefa(x_sei_token, numero_processo, id_unidade)

    resultado = {
        "Info": {
            "Pagina": 1,
            "TotalPaginas": 1,
            "QuantidadeItens": len(andamentos),
            "TotalItens": len(andamentos),
            "NumeroProcesso": numero_processo,
            "Parcial": False,
        },
        "Andamentos": andamentos,
    }

    await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
    return resultado


@router.get("/unidades-abertas/{numero_processo}")
async def sei_unidades_abertas(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
):
    """
    Proxy para consultar unidades com processo aberto.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    cache_key = f"proxy:unidades:{numero_processo}:{id_unidade}"
    cached = await cache.get(cache_key)
    if cached:
        return cached

    data = await consultar_procedimento(x_sei_token, numero_processo, id_unidade)

    resultado = {
        "UnidadesProcedimentoAberto": data.get("UnidadesProcedimentoAberto", []),
        "LinkAcesso": data.get("LinkAcesso"),
    }

    await cache.set(cache_key, resultado, ttl=CACHE_TTL_UNIDADES)
    return resultado


@router.get("/documentos/{numero_processo}")
async def sei_documentos(
    numero_processo: str,
    id_unidade: str = Query(...),
    x_sei_token: str = Header(..., alias="X-SEI-Token"),
    parcial: bool = Query(False),
):
    """
    Proxy para buscar documentos de um processo.
    Se parcial=true e sem cache, retorna primeira+última página imediatamente
    e dispara busca completa em background para preencher o cache.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    cache_key = f"proxy:documentos:{numero_processo}:{id_unidade}"
    logger.info(
        f"GET /sei/documentos/{numero_processo} — "
        f"id_unidade={id_unidade} parcial={parcial}"
    )

    try:
        # Always check full cache first
        cached = await cache.get(cache_key)
        if cached:
            logger.info(
                f"GET /sei/documentos/{numero_processo} — cache HIT "
                f"({cached.get('Info', {}).get('QuantidadeItens', '?')} docs)"
            )
            return cached

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
                await cache.set(cache_key, resultado, ttl=CACHE_TTL_DOCUMENTOS)
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
                        }
                        await cache.set(cache_key, full_resultado, ttl=CACHE_TTL_DOCUMENTOS)
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
        await cache.set(cache_key, resultado, ttl=CACHE_TTL_DOCUMENTOS)
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
    If cache hit, returns a single 'done' event instantly.
    Otherwise streams progress events as batches complete.
    """
    numero_processo = normalizar_numero_processo(numero_processo)
    cache_key = f"proxy:andamentos:{numero_processo}:{id_unidade}"

    # Check cache first
    cached = await cache.get(cache_key)
    if cached:
        logger.info(f"[andamentos-stream] Cache hit for processo={numero_processo}")

        async def cached_generator():
            yield _sse_event({"type": "done", "content": cached})

        return StreamingResponse(
            cached_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    async def stream_generator():
        try:
            all_andamentos = None
            async for event in listar_tarefa_stream(x_sei_token, numero_processo, id_unidade):
                if event["type"] == "progress":
                    yield _sse_event({"type": "progress", "content": {"loaded": event["loaded"], "total": event["total"]}})
                elif event["type"] == "done":
                    all_andamentos = event["andamentos"]
                elif event["type"] == "error":
                    yield _sse_event({"type": "error", "content": event["message"]})
                    return

            if all_andamentos is not None:
                resultado = {
                    "Info": {
                        "Pagina": 1,
                        "TotalPaginas": 1,
                        "QuantidadeItens": len(all_andamentos),
                        "TotalItens": len(all_andamentos),
                        "NumeroProcesso": numero_processo,
                        "Parcial": False,
                    },
                    "Andamentos": all_andamentos,
                }
                await cache.set(cache_key, resultado, ttl=CACHE_TTL_ANDAMENTOS)
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


@router.get("/health")
async def sei_health():
    """
    Verifica saúde da API SEI.
    """
    return await verificar_saude()
