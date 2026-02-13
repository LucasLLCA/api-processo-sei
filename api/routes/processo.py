import asyncio
import json
import logging
from fastapi import APIRouter, Header, HTTPException, Query
from starlette.responses import StreamingResponse
from ..sei import listar_documentos, listar_primeiro_documento, listar_ultimo_documento, listar_ultimos_andamentos, listar_tarefa, consultar_documento, baixar_documento
from ..openai_client import (
    enviar_para_ia_conteudo, enviar_para_ia_conteudo_md, enviar_documento_ia_conteudo,
    enviar_para_ia_conteudo_md_stream, enviar_documento_ia_conteudo_stream,
    enviar_situacao_atual_stream,
)
from ..schemas_legacy import ErrorDetail, ErrorType, Retorno
from ..cache import cache, gerar_chave_processo, gerar_chave_documento, gerar_chave_andamento, gerar_chave_resumo
from ..normalization import normalizar_numero_processo

logger = logging.getLogger(__name__)

router = APIRouter()

# TTL padrão de 48 horas
CACHE_TTL = 172800


@router.get("/andamento/{numero_processo}", response_model=Retorno)
async def andamento(
    numero_processo: str,
    id_unidade: str,
    token: str = Query(default=None),
    x_sei_token: str = Header(default=None, alias="X-SEI-Token"),
):
    """
    Retorna o andamento atual do processo e um resumo do último documento.

    Args:
        numero_processo (str): Número do processo no SEI
        token (str): Token de autenticação do SEI (query param, fallback)
        x_sei_token (str): Token de autenticação do SEI (header, preferred)
        id_unidade (str): ID da unidade no SEI

    Returns:
        Retorno: Objeto contendo o status, andamento e resumo do último documento
    """
    token = x_sei_token or token
    if not token:
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")
    numero_processo = normalizar_numero_processo(numero_processo)
    try:
        # Verifica cache primeiro
        cache_key = gerar_chave_andamento(numero_processo)
        cached_result = await cache.get(cache_key)
        if cached_result:
            logger.debug(f"Retornando andamento do cache para processo {numero_processo}")
            return Retorno(status="ok", andamento=cached_result.get("andamento"), resumo=cached_result.get("resumo"))

        # Busca documentos e andamentos em paralelo
        documentos, andamentos = await asyncio.gather(
            listar_documentos(token, numero_processo, id_unidade),
            listar_tarefa(token, numero_processo, id_unidade)
        )

        if not documentos:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Nenhum documento encontrado para este processo",
                    details={"numero_processo": numero_processo}
                ).dict()
            )

        ultimo = documentos[-1]

        # Consulta documento e baixa conteúdo em paralelo
        doc_ultimo, md_ultimo = await asyncio.gather(
            consultar_documento(token, id_unidade, ultimo["DocumentoFormatado"]),
            baixar_documento(token, id_unidade, ultimo["DocumentoFormatado"], numero_processo)
        )

        # Envia para IA
        resposta_ia_ultimo = await enviar_para_ia_conteudo(md_ultimo) if md_ultimo else {}

        # Armazena no cache
        resultado = {
            "andamento": doc_ultimo,
            "resumo": resposta_ia_ultimo
        }
        await cache.set(cache_key, resultado, ttl=CACHE_TTL)

        return Retorno(
            status="ok",
            andamento=doc_ultimo,
            resumo=resposta_ia_ultimo
        )

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao processar andamento: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o andamento do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )


@router.get("/resumo/{numero_processo}", response_model=Retorno)
async def resumo(
    numero_processo: str,
    id_unidade: str,
    token: str = Query(default=None),
    x_sei_token: str = Header(default=None, alias="X-SEI-Token"),
):
    """
    Retorna um resumo do processo, incluindo o primeiro e último documento.

    Args:
        numero_processo (str): Número do processo no SEI
        token (str): Token de autenticação do SEI (query param, fallback)
        x_sei_token (str): Token de autenticação do SEI (header, preferred)
        id_unidade (str): ID da unidade no SEI

    Returns:
        Retorno: Objeto contendo o status e resumo do processo
    """
    token = x_sei_token or token
    if not token:
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")
    numero_processo = normalizar_numero_processo(numero_processo)
    try:
        # Verifica cache primeiro
        cache_key = gerar_chave_resumo(numero_processo)
        cached_result = await cache.get(cache_key)
        if cached_result:
            logger.debug(f"Retornando resumo do cache para processo {numero_processo}")
            return Retorno(status="ok", resumo=cached_result)

        documentos = await listar_documentos(token, numero_processo, id_unidade)

        if not documentos:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Nenhum documento encontrado para este processo",
                    details={"numero_processo": numero_processo}
                ).dict()
            )

        primeiro = documentos[0]
        ultimo = documentos[-1]

        # Busca todos os dados em paralelo
        doc_primeiro, doc_ultimo, md_primeiro, md_ultimo = await asyncio.gather(
            consultar_documento(token, id_unidade, primeiro["DocumentoFormatado"]),
            consultar_documento(token, id_unidade, ultimo["DocumentoFormatado"]),
            baixar_documento(token, id_unidade, primeiro["DocumentoFormatado"], numero_processo),
            baixar_documento(token, id_unidade, ultimo["DocumentoFormatado"], numero_processo)
        )

        # Envia para IA em paralelo
        resposta_ia_primeiro, resposta_ia_ultimo = await asyncio.gather(
            enviar_para_ia_conteudo(md_primeiro) if md_primeiro else asyncio.sleep(0, result={}),
            enviar_para_ia_conteudo(md_ultimo) if md_ultimo else asyncio.sleep(0, result={})
        )

        resultado = {
            "processo": {
                "numero": numero_processo,
                "id_unidade": id_unidade
            },
            "primeiro_documento": doc_primeiro,
            "resumo_primeiro": resposta_ia_primeiro,
            "ultimo_documento": doc_ultimo,
            "resumo_ultimo": resposta_ia_ultimo
        }

        # Armazena no cache
        await cache.set(cache_key, resultado, ttl=CACHE_TTL)

        return Retorno(status="ok", resumo=resultado)

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao processar resumo: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o resumo do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )


@router.get("/resumo-completo/{numero_processo}", response_model=Retorno)
async def resumo_completo(
    numero_processo: str,
    id_unidade: str,
    token: str = Query(default=None),
    x_sei_token: str = Header(default=None, alias="X-SEI-Token"),
):
    """
    Retorna uma análise completa do processo, combinando o primeiro e último documento.

    Args:
        numero_processo (str): Número do processo no SEI
        token (str): Token de autenticação do SEI (query param, fallback)
        x_sei_token (str): Token de autenticação do SEI (header, preferred)
        id_unidade (str): ID da unidade no SEI

    Returns:
        Retorno: Objeto contendo o status e resumo completo do processo
    """
    token = x_sei_token or token
    if not token:
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")
    numero_processo = normalizar_numero_processo(numero_processo)
    try:
        # Verifica cache primeiro usando apenas o número do processo
        cache_key_base = f"processo:{numero_processo}:resumo_completo"
        cached_result = await cache.get(cache_key_base)
        if cached_result:
            logger.debug(f"Retornando resultado do cache para processo {numero_processo}")
            return Retorno(status="ok", resumo=cached_result)

        # Busca apenas o primeiro documento (não precisa de todos)
        primeiro = await listar_primeiro_documento(token, numero_processo, id_unidade)

        if not primeiro:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Nenhum documento encontrado para este processo",
                    details={"numero_processo": numero_processo}
                ).dict()
            )

        cache_key = cache_key_base

        logger.debug(f"Iniciando processamento do processo {numero_processo}")
        logger.debug(f"Primeiro documento: {primeiro['DocumentoFormatado']}")

        # Busca todos os dados em paralelo
        results = await asyncio.gather(
            consultar_documento(token, id_unidade, primeiro["DocumentoFormatado"]),
            baixar_documento(token, id_unidade, primeiro["DocumentoFormatado"], numero_processo),
            return_exceptions=True
        )

        doc_primeiro = results[0] if not isinstance(results[0], Exception) else {}
        md_primeiro = results[1] if not isinstance(results[1], Exception) else None

        if isinstance(results[0], Exception):
            logger.error(f"Falha ao consultar primeiro documento: {str(results[0])}")
        else:
            logger.debug(f"Primeiro documento consultado: {doc_primeiro.get('Titulo', 'Sem título')}")

        conteudo_combinado = ""
        tipo_arquivo = "html"  # Default

        if md_primeiro:
            try:
                # Detectar tipo de arquivo
                if isinstance(md_primeiro, dict):
                    tipo_arquivo = md_primeiro.get("tipo", "html")
                    conteudo = md_primeiro.get("conteudo")

                    if tipo_arquivo == "html":
                        logger.debug(f"Documento HTML - Conteúdo: {len(conteudo)} caracteres")
                        conteudo_combinado += f"PRIMEIRO DOCUMENTO:\n{conteudo}\n\n"
                    elif tipo_arquivo == "pdf":
                        logger.debug(f"Documento PDF - Tamanho: {len(conteudo)} bytes")
                        conteudo_combinado = conteudo  # Para PDF, usar o binário direto
                else:
                    # Formato antigo (compatibilidade)
                    conteudo_combinado += f"PRIMEIRO DOCUMENTO:\n{md_primeiro}\n\n"

            except Exception as e:
                logger.error(f"Falha ao processar primeiro documento: {str(e)}")

        logger.debug(f"Tipo de arquivo detectado: {tipo_arquivo}")

        try:
            if conteudo_combinado:
                resposta_ia_combinada = await enviar_para_ia_conteudo_md(conteudo_combinado, tipo_arquivo)
                logger.debug(f"Resposta da IA recebida: {resposta_ia_combinada.get('status', 'sem status')}")
            else:
                resposta_ia_combinada = {}
        except Exception as e:
            logger.error(f"Falha ao obter resposta da IA: {str(e)}")
            resposta_ia_combinada = {"status": "erro", "resposta_ia": f"Erro ao processar: {str(e)}"}

        # Monta o resultado
        resultado = {
            "processo": {
                "numero": numero_processo,
                "id_unidade": id_unidade
            },
            "primeiro_documento": doc_primeiro,
            "resumo_combinado": resposta_ia_combinada
        }

        # Armazena no cache
        await cache.set(cache_key, resultado, ttl=CACHE_TTL)

        return Retorno(status="ok", resumo=resultado)

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao processar resumo completo: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o resumo completo do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )


@router.get("/resumo-documento/{documento_formatado}", response_model=Retorno)
async def resumo_documento(
    documento_formatado: str,
    id_unidade: str,
    token: str = Query(default=None),
    x_sei_token: str = Header(default=None, alias="X-SEI-Token"),
):
    """
    Retorna uma análise completa de um documento.

    Args:
        documento_formatado (str): Número do documento formatado
        token (str): Token de autenticação do SEI (query param, fallback)
        x_sei_token (str): Token de autenticação do SEI (header, preferred)
        id_unidade (str): ID da unidade no SEI

    Returns:
        Retorno: Objeto contendo o status e resumo completo do documento
    """
    token = x_sei_token or token
    if not token:
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")
    try:
        # Gera chave de cache com o ID do documento
        cache_key = gerar_chave_documento(documento_formatado)

        # Tenta obter do cache
        cached_result = await cache.get(cache_key)
        if cached_result:
            logger.debug(f"Retornando resultado do cache para documento {documento_formatado}")
            return Retorno(status="ok", resumo=cached_result)

        # Busca documento e conteúdo em paralelo
        doc, md = await asyncio.gather(
            consultar_documento(token, id_unidade, documento_formatado),
            baixar_documento(token, id_unidade, documento_formatado)
        )

        conteudo = ""
        tipo_arquivo = "html"  # Default

        if md:
            # Detectar tipo de arquivo
            if isinstance(md, dict):
                tipo_arquivo = md.get("tipo", "html")
                conteudo_raw = md.get("conteudo")

                if tipo_arquivo == "html":
                    conteudo = conteudo_raw
                    logger.debug(f"Documento HTML - Conteúdo: {len(conteudo)} caracteres")
                elif tipo_arquivo == "pdf":
                    conteudo = conteudo_raw  # Para PDF, manter binário
                    logger.debug(f"Documento PDF - Tamanho: {len(conteudo)} bytes")
            else:
                # Formato antigo (compatibilidade)
                conteudo = md
        else:
            logger.warning(f"Documento {documento_formatado} não retornou conteúdo MD, usando dados básicos")

        if not conteudo:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Documento não encontrado ou não foi possível processar o conteúdo",
                    details={"documento_formatado": documento_formatado}
                ).dict()
            )

        logger.debug(f"Tipo de arquivo detectado: {tipo_arquivo}")
        resposta_ia = await enviar_documento_ia_conteudo(conteudo, tipo_arquivo)

        # Monta o resultado
        resultado = {
            "documento": doc,
            "resumo": resposta_ia
        }

        # Armazena no cache
        await cache.set(cache_key, resultado, ttl=CACHE_TTL)

        return Retorno(status="ok", resumo=resultado)

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao processar resumo do documento: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o resumo do documento",
                details={"error": str(e), "documento_formatado": documento_formatado}
            ).dict()
        )


def _sse_event(data: dict) -> str:
    """Formata um evento SSE."""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


@router.get("/resumo-completo-stream/{numero_processo}")
async def resumo_completo_stream(
    numero_processo: str,
    id_unidade: str,
    token: str = Query(default=None),
    x_sei_token: str = Header(default=None, alias="X-SEI-Token"),
):
    """
    Versão streaming (SSE) do resumo-completo.
    Retorna chunks progressivos via Server-Sent Events.
    Se houver cache, retorna o resultado completo em um único evento 'done'.
    """
    token = x_sei_token or token
    if not token:
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")
    numero_processo = normalizar_numero_processo(numero_processo)

    cache_key = f"processo:{numero_processo}:resumo_completo"

    # Verifica cache - se hit, retorna instantaneamente
    cached_result = await cache.get(cache_key)
    if cached_result:
        logger.debug(f"[stream] Retornando resumo do cache para processo {numero_processo}")

        async def cached_generator():
            yield _sse_event({"type": "done", "content": cached_result})

        return StreamingResponse(
            cached_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # Cache miss - buscar documento e streamer a resposta da IA
    async def stream_generator():
        try:
            primeiro = await listar_primeiro_documento(token, numero_processo, id_unidade)

            if not primeiro:
                yield _sse_event({"type": "error", "content": "Nenhum documento encontrado para este processo"})
                return

            results = await asyncio.gather(
                consultar_documento(token, id_unidade, primeiro["DocumentoFormatado"]),
                baixar_documento(token, id_unidade, primeiro["DocumentoFormatado"], numero_processo),
                return_exceptions=True,
            )

            doc_primeiro = results[0] if not isinstance(results[0], Exception) else {}
            md_primeiro = results[1] if not isinstance(results[1], Exception) else None

            conteudo_combinado = ""
            tipo_arquivo = "html"

            if md_primeiro:
                if isinstance(md_primeiro, dict):
                    tipo_arquivo = md_primeiro.get("tipo", "html")
                    conteudo = md_primeiro.get("conteudo")
                    if tipo_arquivo == "html":
                        conteudo_combinado += f"PRIMEIRO DOCUMENTO:\n{conteudo}\n\n"
                    elif tipo_arquivo == "pdf":
                        conteudo_combinado = conteudo
                else:
                    conteudo_combinado += f"PRIMEIRO DOCUMENTO:\n{md_primeiro}\n\n"

            if not conteudo_combinado:
                yield _sse_event({"type": "error", "content": "Não foi possível extrair conteúdo do documento"})
                return

            accumulated = []
            async for chunk in enviar_para_ia_conteudo_md_stream(conteudo_combinado, tipo_arquivo):
                accumulated.append(chunk)
                yield _sse_event({"type": "chunk", "content": chunk})

            full_text = "".join(accumulated)
            resposta_ia = {"status": "ok", "resposta_ia": full_text}

            resultado = {
                "processo": {"numero": numero_processo, "id_unidade": id_unidade},
                "primeiro_documento": doc_primeiro,
                "resumo_combinado": resposta_ia,
            }

            await cache.set(cache_key, resultado, ttl=CACHE_TTL)

            yield _sse_event({"type": "done", "content": resultado})

        except Exception as e:
            logger.error(f"[stream] Erro ao processar resumo completo: {str(e)}", exc_info=True)
            yield _sse_event({"type": "error", "content": f"Erro ao processar resumo: {str(e)}"})

    return StreamingResponse(
        stream_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/resumo-documento-stream/{documento_formatado}")
async def resumo_documento_stream(
    documento_formatado: str,
    id_unidade: str,
    token: str = Query(default=None),
    x_sei_token: str = Header(default=None, alias="X-SEI-Token"),
):
    """
    Versão streaming (SSE) do resumo-documento.
    Retorna chunks progressivos via Server-Sent Events.
    """
    token = x_sei_token or token
    if not token:
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")

    cache_key = gerar_chave_documento(documento_formatado)

    cached_result = await cache.get(cache_key)
    if cached_result:
        logger.debug(f"[stream] Retornando resumo do cache para documento {documento_formatado}")

        async def cached_generator():
            yield _sse_event({"type": "done", "content": cached_result})

        return StreamingResponse(
            cached_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    async def stream_generator():
        try:
            doc, md = await asyncio.gather(
                consultar_documento(token, id_unidade, documento_formatado),
                baixar_documento(token, id_unidade, documento_formatado),
            )

            conteudo = ""
            tipo_arquivo = "html"

            if md:
                if isinstance(md, dict):
                    tipo_arquivo = md.get("tipo", "html")
                    conteudo = md.get("conteudo")
                else:
                    conteudo = md

            if not conteudo:
                yield _sse_event({"type": "error", "content": "Documento não encontrado ou conteúdo vazio"})
                return

            accumulated = []
            async for chunk in enviar_documento_ia_conteudo_stream(conteudo, tipo_arquivo):
                accumulated.append(chunk)
                yield _sse_event({"type": "chunk", "content": chunk})

            full_text = "".join(accumulated)
            resposta_ia = {"status": "ok", "resposta_ia": full_text}

            resultado = {
                "documento": doc,
                "resumo": resposta_ia,
            }

            await cache.set(cache_key, resultado, ttl=CACHE_TTL)

            yield _sse_event({"type": "done", "content": resultado})

        except Exception as e:
            logger.error(f"[stream] Erro ao processar resumo do documento: {str(e)}", exc_info=True)
            yield _sse_event({"type": "error", "content": f"Erro ao processar resumo: {str(e)}"})

    return StreamingResponse(
        stream_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/resumo-situacao-stream/{numero_processo}")
async def resumo_situacao_stream(
    numero_processo: str,
    id_unidade: str,
    token: str = Query(default=None),
    x_sei_token: str = Header(default=None, alias="X-SEI-Token"),
):
    """
    Versão streaming (SSE) da situação atual do processo.
    Combina o entendimento existente, último documento e últimos andamentos
    para gerar um resumo da situação corrente.
    """
    token = x_sei_token or token
    if not token:
        raise HTTPException(status_code=401, detail="Token de autenticação não fornecido")
    numero_processo = normalizar_numero_processo(numero_processo)

    cache_key = f"processo:{numero_processo}:situacao_atual"

    cached_result = await cache.get(cache_key)
    if cached_result:
        logger.debug(f"[stream] Retornando situação atual do cache para processo {numero_processo}")

        async def cached_generator():
            yield _sse_event({"type": "done", "content": cached_result})

        return StreamingResponse(
            cached_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    async def stream_generator():
        try:
            # 1. Get cached entendimento
            resumo_cache_key = f"processo:{numero_processo}:resumo_completo"
            cached_resumo = await cache.get(resumo_cache_key)
            entendimento = ""
            if cached_resumo:
                entendimento = cached_resumo.get("resumo_combinado", {}).get("resposta_ia", "")

            if not entendimento:
                yield _sse_event({"type": "error", "content": "Entendimento do processo não disponível. Gere o entendimento primeiro."})
                return

            # 2. Fetch last document + last 3 andamentos in parallel (optimized)
            ultimo_doc, ultimos_andamentos = await asyncio.gather(
                listar_ultimo_documento(token, numero_processo, id_unidade),
                listar_ultimos_andamentos(token, numero_processo, id_unidade, quantidade=3),
            )

            # 3. Process last document content with smart caching
            ultimo_doc_conteudo = ""
            if ultimo_doc:
                doc_id = ultimo_doc.get("DocumentoFormatado", "")
                doc_cache_key = f"processo:{numero_processo}:ultimo_doc:{doc_id}"

                # Check document-level cache first
                cached_doc_content = await cache.get(doc_cache_key)
                if cached_doc_content:
                    ultimo_doc_conteudo = cached_doc_content
                    logger.debug(f"[stream] Último documento {doc_id} retornado do cache")
                else:
                    md = await baixar_documento(token, id_unidade, doc_id, numero_processo)
                    if md:
                        if isinstance(md, dict):
                            tipo = md.get("tipo", "html")
                            if tipo == "html":
                                ultimo_doc_conteudo = md.get("conteudo", "")
                            else:
                                ultimo_doc_conteudo = "(Documento PDF - conteúdo binário não disponível em texto)"
                        else:
                            ultimo_doc_conteudo = md
                        # Cache document content by doc_id
                        if ultimo_doc_conteudo:
                            await cache.set(doc_cache_key, ultimo_doc_conteudo, ttl=CACHE_TTL)

            # 4. Format andamentos text
            andamentos_texto = "\n".join(
                f"- {a.get('DataHora', '')} | {a.get('Unidade', {}).get('Sigla', '')} | {a.get('Descricao', '')}"
                for a in ultimos_andamentos
            )

            # 5. Stream the response
            accumulated = []
            async for chunk in enviar_situacao_atual_stream(entendimento, ultimo_doc_conteudo, andamentos_texto):
                accumulated.append(chunk)
                yield _sse_event({"type": "chunk", "content": chunk})

            full_text = "".join(accumulated)

            resultado = {
                "status": "ok",
                "situacao_atual": full_text,
            }

            await cache.set(cache_key, resultado, ttl=CACHE_TTL)

            yield _sse_event({"type": "done", "content": resultado})

        except Exception as e:
            logger.error(f"[stream] Erro ao processar situação atual: {str(e)}", exc_info=True)
            yield _sse_event({"type": "error", "content": f"Erro ao processar situação atual: {str(e)}"})

    return StreamingResponse(
        stream_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
