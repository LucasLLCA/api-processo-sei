import re
import time
import httpx
import math
import asyncio
import logging
from opentelemetry import trace
from fastapi import HTTPException
from .schemas_legacy import ErrorDetail, ErrorType
from .utils import converte_html_para_markdown_memoria
from .config import settings
from .telemetry import sei_retry_counter, sei_request_duration

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("api.sei")

# Cliente HTTP global com connection pool
http_client = httpx.AsyncClient(
    timeout=httpx.Timeout(180.0, connect=30.0),
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    http2=True,
    verify=False
)

# Fixed retry config: 120s per attempt (SEI docs endpoint takes ~80s), 2s backoff
RETRY_TIMEOUT = 180
RETRY_BACKOFF = 2


async def _fazer_requisicao_com_retry(url: str, headers: dict, params: dict, max_tentativas: int = 3, timeout: int = RETRY_TIMEOUT):
    """
    Faz uma requisição HTTP com retry automático em caso de falha de rede.
    Retorna imediatamente para respostas HTTP (inclusive 4xx/5xx).
    Só faz retry em timeout e erros de conexão.
    Fixed 60s timeout per attempt, 2s backoff between retries.
    """
    # Extract URL path for metric labels (avoid leaking full URL with tokens)
    url_path = url.replace(settings.SEI_BASE_URL, "")
    with tracer.start_as_current_span("sei.api_call_with_retry", attributes={
        "sei.url_path": url_path,
        "sei.max_retries": max_tentativas,
        "sei.timeout_per_attempt": timeout,
    }) as span:
        start = time.monotonic()
        for tentativa in range(max_tentativas):
            try:
                response = await http_client.get(url, headers=headers, params=params, timeout=timeout)
                elapsed = time.monotonic() - start
                span.set_attribute("sei.attempt_count", tentativa + 1)
                span.set_attribute("sei.response.status_code", response.status_code)
                sei_request_duration.record(elapsed, {"sei.url_path": url_path})
                if response.status_code >= 400:
                    logger.warning(
                        f"HTTP {response.status_code} "
                        f"GET {url} params={params} — body={response.text[:500]}"
                    )
                return response
            except httpx.TimeoutException as e:
                sei_retry_counter.add(1, {"sei.url_path": url_path, "sei.reason": "timeout"})
                span.add_event("sei.retry", {"attempt": tentativa + 1, "reason": "timeout"})
                logger.warning(
                    f"TIMEOUT na tentativa {tentativa + 1}/{max_tentativas} "
                    f"(timeout={timeout}s) "
                    f"GET {url} params={params} — {type(e).__name__}: {e}"
                )
                if tentativa == max_tentativas - 1:
                    span.set_status(trace.StatusCode.ERROR, "SEI timeout after retries")
                    span.record_exception(e)
                    raise e
                await asyncio.sleep(RETRY_BACKOFF)
            except httpx.ConnectError as e:
                sei_retry_counter.add(1, {"sei.url_path": url_path, "sei.reason": "connect_error"})
                span.add_event("sei.retry", {"attempt": tentativa + 1, "reason": "connect_error"})
                logger.warning(
                    f"CONNECT_ERROR na tentativa {tentativa + 1}/{max_tentativas} "
                    f"GET {url} params={params} — {type(e).__name__}: {e}"
                )
                if tentativa == max_tentativas - 1:
                    span.set_status(trace.StatusCode.ERROR, "SEI connect error after retries")
                    span.record_exception(e)
                    raise e
                await asyncio.sleep(RETRY_BACKOFF)
            except httpx.RequestError as e:
                span.set_status(trace.StatusCode.ERROR, str(e))
                span.record_exception(e)
                logger.error(
                    f"REQUEST_ERROR (não retentável) "
                    f"GET {url} params={params} — {type(e).__name__}: {e}"
                )
                raise e


def _extrair_mensagem_sei(response) -> str:
    """Extrai mensagem de erro da resposta SEI, se disponível."""
    try:
        data = response.json()
        detail = data.get("detail", [])
        if isinstance(detail, list) and detail:
            return detail[0].get("msg", "")
    except Exception:
        pass
    return ""


def _raise_sei_error(response, fallback_message: str):
    """
    Levanta HTTPException com status code apropriado baseado na resposta SEI.
    - 422 do SEI (processo não encontrado, acesso negado) → 422 para o cliente
    - 401/403 → passa direto
    - Outros → 502 (upstream error)
    """
    sei_msg = _extrair_mensagem_sei(response)
    status = response.status_code

    if status in (401, 403, 422):
        raise HTTPException(
            status_code=status,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message=sei_msg or fallback_message,
                details={"status_code": status, "response": response.text[:500]}
            ).dict()
        )

    raise HTTPException(
        status_code=502,
        detail=ErrorDetail(
            type=ErrorType.EXTERNAL_SERVICE_ERROR,
            message=fallback_message,
            details={"status_code": status, "response": response.text[:500]}
        ).dict()
    )


async def _buscar_pagina_documentos(token: str, protocolo: str, id_unidade: str, pagina: int, quantidade_por_pagina: int):
    """
    Função auxiliar para buscar uma página específica de documentos com retry
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
            "pagina": pagina,
            "quantidade": quantidade_por_pagina,
            "sinal_geracao": "N",
            "sinal_assinaturas": "N",
            "sinal_publicacao": "N",
            "sinal_campos": "N",
            "sinal_completo": "S"
        }
        headers = {"accept": "application/json", "token": token}

        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            logger.warning(
                f"HTTP {response.status_code} na página {pagina} de documentos "
                f"processo={protocolo} unidade={id_unidade} — body={response.text[:300]}"
            )
            return []

        return response.json().get("Documentos", [])
    except httpx.TimeoutException as e:
        logger.warning(f"TIMEOUT na página {pagina} de documentos processo={protocolo} — {type(e).__name__}: {e}")
        return []
    except httpx.ConnectError as e:
        logger.warning(f"CONNECT_ERROR na página {pagina} de documentos processo={protocolo} — {type(e).__name__}: {e}")
        return []
    except httpx.RequestError as e:
        logger.warning(f"REQUEST_ERROR na página {pagina} de documentos processo={protocolo} — {type(e).__name__}: {e}")
        return []
    except Exception as e:
        logger.warning(f"UNEXPECTED_ERROR na página {pagina} de documentos processo={protocolo} — {type(e).__name__}: {e}")
        return []


async def listar_documentos(token: str, protocolo: str, id_unidade: str):
    """
    Fetch all documents for a processo.
    Strategy: first page + last page upfront, then middle pages in batches of 10.
    Uses quantidade=10 per page to avoid SEI API timeouts on large processes.
    """
    try:
        # First request: page 1 (discovery)
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
            "pagina": 1,
            "quantidade": 10,
            "sinal_geracao": "N",
            "sinal_assinaturas": "N",
            "sinal_publicacao": "N",
            "sinal_campos": "N",
            "sinal_completo": "S"
        }
        headers = {"accept": "application/json", "token": token}
        logger.debug(f"Fazendo requisição inicial de documentos para processo: {protocolo}")
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao listar documentos no SEI")

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        documentos_primeira_pagina = data.get("Documentos", [])

        if total_itens == 0:
            return []

        if total_itens <= 10:
            return documentos_primeira_pagina

        quantidade_por_pagina = 10
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)
        batch_size = 10
        max_retries = 3

        logger.info(
            f"Documentos full fetch: processo={protocolo} total={total_itens} "
            f"paginas={total_paginas} batch_size={batch_size}"
        )

        # Fetch last page first (alongside page 1 already fetched)
        resultados_por_pagina = {1: documentos_primeira_pagina}

        if total_paginas > 1:
            ultima_pagina_docs = await _buscar_pagina_documentos(
                token, protocolo, id_unidade, total_paginas, quantidade_por_pagina
            )
            if isinstance(ultima_pagina_docs, Exception) or ultima_pagina_docs == []:
                logger.warning(f"Última página ({total_paginas}) falhou na primeira tentativa, será retentada")
            else:
                resultados_por_pagina[total_paginas] = ultima_pagina_docs

        # Middle pages: everything between first and last
        paginas_pendentes = [
            p for p in range(2, total_paginas)
            if p not in resultados_por_pagina
        ]
        # Add last page back if it failed
        if total_paginas not in resultados_por_pagina and total_paginas > 1:
            paginas_pendentes.append(total_paginas)

        for tentativa in range(max_retries):
            if not paginas_pendentes:
                break

            if tentativa > 0:
                logger.info(f"Retry {tentativa}/{max_retries}: refazendo {len(paginas_pendentes)} páginas de documentos falhadas")

            paginas_falhadas = []

            for i in range(0, len(paginas_pendentes), batch_size):
                batch = paginas_pendentes[i:i + batch_size]
                tarefas = [
                    _buscar_pagina_documentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
                    for pagina in batch
                ]

                resultados = await asyncio.gather(*tarefas, return_exceptions=True)

                for j, resultado in enumerate(resultados):
                    pagina = batch[j]
                    if isinstance(resultado, Exception) or resultado == []:
                        paginas_falhadas.append(pagina)
                        logger.warning(f"Página {pagina} de documentos falhou (tentativa {tentativa + 1})")
                    else:
                        resultados_por_pagina[pagina] = resultado

                logger.debug(
                    f"Lote documentos páginas {batch[0]}-{batch[-1]} concluído "
                    f"(tentativa {tentativa + 1}): {len(resultados_por_pagina)}/{total_paginas} ok"
                )

            paginas_pendentes = paginas_falhadas

        if paginas_pendentes:
            logger.error(
                f"Paginação de documentos incompleta após {max_retries} tentativas. "
                f"Páginas faltando: {paginas_pendentes}. "
                f"Coletadas: {len(resultados_por_pagina)}/{total_paginas}"
            )
            raise HTTPException(
                status_code=502,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message=f"Falha ao buscar todas as páginas de documentos. {len(paginas_pendentes)} páginas falharam após {max_retries} tentativas.",
                    details={"paginas_falhadas": paginas_pendentes}
                ).dict()
            )

        # Combine in page order
        todos_documentos = []
        for pagina in sorted(resultados_por_pagina.keys()):
            todos_documentos.extend(resultados_por_pagina[pagina])

        logger.info(f"Documentos full fetch complete: processo={protocolo} total={len(todos_documentos)}")

        return todos_documentos
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para listar documentos",
                details={"error": str(e)}
            ).dict()
        )


async def listar_primeiro_documento(token: str, protocolo: str, id_unidade: str):
    """
    Busca apenas o primeiro documento de um processo.
    Usado pelo resumo_completo que só precisa do primeiro documento.
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
            "pagina": 1,
            "quantidade": 1,
            "sinal_geracao": "N",
            "sinal_assinaturas": "N",
            "sinal_publicacao": "N",
            "sinal_campos": "N",
            "sinal_completo": "S"
        }
        headers = {"accept": "application/json", "token": token}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao listar documentos no SEI")

        documentos = response.json().get("Documentos", [])
        return documentos[0] if documentos else None
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para listar documentos",
                details={"error": str(e)}
            ).dict()
        )


async def listar_ultimo_documento(token: str, protocolo: str, id_unidade: str):
    """
    Busca apenas o último documento de um processo.
    Usado pela situação atual que só precisa do último documento.
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
            "pagina": 1,
            "quantidade": 1,
            "sinal_geracao": "N",
            "sinal_assinaturas": "N",
            "sinal_publicacao": "N",
            "sinal_campos": "N",
            "sinal_completo": "S"
        }
        headers = {"accept": "application/json", "token": token}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            logger.warning(f"Falha ao listar documentos para último documento (status {response.status_code})")
            return None

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        documentos = data.get("Documentos", [])

        if total_itens == 0:
            return None

        # Se só existe 1 documento, já o temos
        if total_itens <= 1:
            return documentos[0] if documentos else None

        # Buscar a última página (qty=1, page=TotalItens => último item)
        params["pagina"] = total_itens
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            logger.warning(f"Falha ao buscar último documento na página {total_itens}")
            return None

        documentos = response.json().get("Documentos", [])
        return documentos[0] if documentos else None
    except Exception as e:
        logger.warning(f"Erro ao buscar último documento: {str(e)}")
        return None


async def listar_ultimos_andamentos(token: str, protocolo: str, id_unidade: str, quantidade: int = 3):
    """
    Busca apenas os últimos N andamentos de um processo.
    Retorna lista vazia em caso de erro (graceful degradation).
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_atributos": "S",
            "pagina": 1,
            "quantidade": 10
        }
        headers = {"accept": "application/json", "token": token}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            logger.warning(f"Falha ao listar andamentos para últimos {quantidade} (status {response.status_code})")
            return []

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        andamentos = data.get("Andamentos", [])

        if total_itens == 0:
            return []

        # Se tudo cabe na primeira página, retornar os últimos N
        if total_itens <= 10:
            return andamentos[-quantidade:]

        # Calcular última página
        quantidade_por_pagina = 10
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)

        # Buscar última página
        params["pagina"] = total_paginas
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            logger.warning(f"Falha ao buscar última página de andamentos")
            return []

        ultima_pagina_andamentos = response.json().get("Andamentos", [])

        # Se a última página tem itens suficientes, retornar os últimos N
        if len(ultima_pagina_andamentos) >= quantidade:
            return ultima_pagina_andamentos[-quantidade:]

        # Precisamos da penúltima página também
        if total_paginas >= 2:
            params["pagina"] = total_paginas - 1
            response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

            if response.status_code == 200:
                penultima = response.json().get("Andamentos", [])
                combinados = penultima + ultima_pagina_andamentos
                return combinados[-quantidade:]

        return ultima_pagina_andamentos[-quantidade:]
    except Exception as e:
        logger.warning(f"Erro ao buscar últimos andamentos: {str(e)}")
        return []


async def buscar_pagina_andamentos(token: str, protocolo: str, id_unidade: str, pagina: int, quantidade_por_pagina: int):
    """
    Função auxiliar para buscar uma página específica de andamentos com retry
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_atributos": "S",
            "pagina": pagina,
            "quantidade": quantidade_por_pagina
        }
        headers = {"accept": "application/json", "token": token}

        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            logger.warning(
                f"HTTP {response.status_code} na página {pagina} de andamentos "
                f"processo={protocolo} unidade={id_unidade} — body={response.text[:300]}"
            )
            return []

        return response.json().get("Andamentos", [])
    except httpx.TimeoutException as e:
        logger.warning(f"TIMEOUT na página {pagina} de andamentos processo={protocolo} — {type(e).__name__}: {e}")
        return []
    except httpx.ConnectError as e:
        logger.warning(f"CONNECT_ERROR na página {pagina} de andamentos processo={protocolo} — {type(e).__name__}: {e}")
        return []
    except httpx.RequestError as e:
        logger.warning(f"REQUEST_ERROR na página {pagina} de andamentos processo={protocolo} — {type(e).__name__}: {e}")
        return []
    except Exception as e:
        logger.warning(f"UNEXPECTED_ERROR na página {pagina} de andamentos processo={protocolo} — {type(e).__name__}: {e}")
        return []


async def contar_andamentos(token: str, protocolo: str, id_unidade: str) -> int:
    """
    Metadata-only request with quantidade=0 to discover TotalItens
    without transferring any andamento data.
    Uses a short-lived Redis cache (60s) to deduplicate concurrent calls
    from multiple endpoints hitting this for the same process.
    """
    from .cache import cache as _cache

    cache_key = f"andamentos_count:{protocolo}:{id_unidade}"
    cached = await _cache.get(cache_key)
    if cached is not None:
        logger.info(f"[contar_andamentos] cache HIT processo={protocolo} total={cached}")
        return cached

    url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
    headers = {"accept": "application/json", "token": token}
    response = await _fazer_requisicao_com_retry(
        url, headers,
        {"protocolo_procedimento": protocolo, "sinal_atributos": "S", "pagina": 1, "quantidade": 0},
        max_tentativas=3,
    )
    if response.status_code != 200:
        _raise_sei_error(response, "Falha ao consultar andamentos no SEI")
    total = response.json().get("Info", {}).get("TotalItens", 0)

    await _cache.set(cache_key, total, ttl=172800)
    logger.info(f"[contar_andamentos] cache MISS processo={protocolo} total={total} (cached 48h)")
    return total


async def listar_tarefa_parcial(token: str, protocolo: str, id_unidade: str):
    """
    Fast initial fetch of andamentos using a two-step strategy:
    1. quantidade=0 request to discover TotalItens without transferring data
    2. One or two parallel fetches with quantidade=40 depending on total size

    Returns (andamentos, total_itens, parcial) tuple.
    - TotalItens <= 40:  1 fetch, parcial=False (all data)
    - TotalItens <= 80:  1 fetch (40 items), parcial=True (rest in background)
    - TotalItens > 80:   2 parallel fetches (80 items), parcial=True
    """
    FETCH_SIZE = 40

    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        headers = {"accept": "application/json", "token": token}

        # Step 1: Metadata-only request to discover total items count
        response = await _fazer_requisicao_com_retry(
            url, headers,
            {"protocolo_procedimento": protocolo, "sinal_atributos": "S", "pagina": 1, "quantidade": 0},
            max_tentativas=3,
        )

        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao consultar andamentos no SEI")

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)

        if total_itens == 0:
            return [], 0, False

        # Step 2: Fetch initial data based on total size
        if total_itens <= FETCH_SIZE:
            # Single fetch gets everything
            andamentos = await buscar_pagina_andamentos(token, protocolo, id_unidade, 1, FETCH_SIZE)
            return andamentos, total_itens, False

        if total_itens <= FETCH_SIZE * 2:
            # Single fetch for fast initial render, rest in background
            logger.info(
                f"Partial fetch (1x{FETCH_SIZE}): processo={protocolo} total_itens={total_itens}"
            )
            andamentos = await buscar_pagina_andamentos(token, protocolo, id_unidade, 1, FETCH_SIZE)
            return andamentos, total_itens, True

        # TotalItens > 80: two parallel fetches for fast initial render
        logger.info(
            f"Partial fetch (2x{FETCH_SIZE}): processo={protocolo} total_itens={total_itens}"
        )
        page1, page2 = await asyncio.gather(
            buscar_pagina_andamentos(token, protocolo, id_unidade, 1, FETCH_SIZE),
            buscar_pagina_andamentos(token, protocolo, id_unidade, 2, FETCH_SIZE),
        )
        andamentos = page1 + page2
        return andamentos, total_itens, True

    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para consultar andamentos",
                details={"error": str(e)}
            ).dict()
        )


async def listar_documentos_parcial(token: str, protocolo: str, id_unidade: str):
    """
    Fetch first page + last page of documents for fast initial render.
    Uses quantidade=10 per page. Returns only first+last page for any
    process with >10 documents; the rest is fetched in background via
    listar_documentos (batch_size=10).
    Returns (documentos, total_itens, parcial) tuple.
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
            "pagina": 1,
            "quantidade": 10,
            "sinal_geracao": "N",
            "sinal_assinaturas": "N",
            "sinal_publicacao": "N",
            "sinal_campos": "N",
            "sinal_completo": "S"
        }
        headers = {"accept": "application/json", "token": token}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao listar documentos no SEI")

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        documentos_primeira_pagina = data.get("Documentos", [])

        if total_itens == 0:
            return [], 0, False

        # Small process: everything fits in one page
        if total_itens <= 10:
            return documentos_primeira_pagina, total_itens, False

        # >10 docs: fetch only first + last page, mark as partial
        quantidade_por_pagina = 10
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)

        logger.info(
            f"Partial docs fetch: processo={protocolo} total_paginas={total_paginas} "
            f"total_itens={total_itens} — fetching first + last page only"
        )

        ultima_pagina_docs = await _buscar_pagina_documentos(
            token, protocolo, id_unidade, total_paginas, quantidade_por_pagina
        )

        documentos = documentos_primeira_pagina + ultima_pagina_docs
        logger.info(
            f"Partial docs fetch complete: processo={protocolo} "
            f"returned {len(documentos)} items (first+last of {total_itens})"
        )
        return documentos, total_itens, True

    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para listar documentos",
                details={"error": str(e)}
            ).dict()
        )


async def listar_tarefa(token: str, protocolo: str, id_unidade: str):
    try:
        # Primeira requisição para descobrir o total de itens
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_atributos": "S",
            "pagina": 1,
            "quantidade": 10
        }
        headers = {"accept": "application/json", "token": token}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao consultar andamentos no SEI")

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        andamentos_primeira_pagina = data.get("Andamentos", [])

        if total_itens == 0:
            return []

        # Se total é pequeno, já temos todos os andamentos
        if total_itens <= 10:
            return andamentos_primeira_pagina

        # Para totais maiores, usar paginação paralela em lotes com retry
        quantidade_por_pagina = 10
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)
        batch_size = 20
        max_retries = 3

        logger.info(
            f"Andamentos full fetch: processo={protocolo} total={total_itens} "
            f"paginas={total_paginas} batch_size={batch_size}"
        )

        # Fetch first page (already have) + last page upfront
        resultados_por_pagina = {1: andamentos_primeira_pagina}

        if total_paginas > 1:
            ultima_pagina = await buscar_pagina_andamentos(
                token, protocolo, id_unidade, total_paginas, quantidade_por_pagina
            )
            if isinstance(ultima_pagina, Exception) or ultima_pagina == []:
                logger.warning(f"Última página ({total_paginas}) falhou na primeira tentativa, será retentada")
            else:
                resultados_por_pagina[total_paginas] = ultima_pagina

        # Middle pages: everything between first and last
        paginas_pendentes = [
            p for p in range(2, total_paginas)
            if p not in resultados_por_pagina
        ]
        # Add last page back if it failed
        if total_paginas not in resultados_por_pagina and total_paginas > 1:
            paginas_pendentes.append(total_paginas)

        for tentativa in range(max_retries):
            if not paginas_pendentes:
                break

            if tentativa > 0:
                logger.info(f"Retry {tentativa}/{max_retries}: refazendo {len(paginas_pendentes)} páginas falhadas")

            paginas_falhadas = []

            for i in range(0, len(paginas_pendentes), batch_size):
                batch = paginas_pendentes[i:i + batch_size]
                tarefas = [
                    buscar_pagina_andamentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
                    for pagina in batch
                ]

                resultados = await asyncio.gather(*tarefas, return_exceptions=True)

                for j, resultado in enumerate(resultados):
                    pagina = batch[j]
                    if isinstance(resultado, Exception) or resultado == []:
                        paginas_falhadas.append(pagina)
                        logger.warning(f"Página {pagina} falhou (tentativa {tentativa + 1})")
                    else:
                        resultados_por_pagina[pagina] = resultado

                logger.debug(
                    f"Lote páginas {batch[0]}-{batch[-1]} concluído "
                    f"(tentativa {tentativa + 1}): {len(resultados_por_pagina)}/{total_paginas} ok"
                )

            paginas_pendentes = paginas_falhadas

        if paginas_pendentes:
            logger.error(
                f"Paginação incompleta após {max_retries} tentativas. "
                f"Páginas faltando: {paginas_pendentes}. "
                f"Coletadas: {len(resultados_por_pagina)}/{total_paginas}"
            )
            raise HTTPException(
                status_code=502,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message=f"Falha ao buscar todas as páginas de andamentos. {len(paginas_pendentes)} páginas falharam após {max_retries} tentativas.",
                    details={"paginas_falhadas": paginas_pendentes}
                ).dict()
            )

        # Combine in page order
        todas_tarefas = []
        for pagina in sorted(resultados_por_pagina.keys()):
            todas_tarefas.extend(resultados_por_pagina[pagina])

        logger.debug(f"Paginação concluída. Total esperado: {total_itens}, Total coletado: {len(todas_tarefas)}")
        return todas_tarefas
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para consultar andamentos",
                details={"error": str(e)}
            ).dict()
        )


async def listar_tarefa_stream(token: str, protocolo: str, id_unidade: str):
    """
    Async generator version of listar_tarefa that yields progress events
    after each batch of pages completes. Used by the SSE endpoint.
    Yields dicts: {"type": "progress", "loaded": N, "total": T}
                  {"type": "done", "andamentos": [...]}
                  {"type": "error", "message": "..."}
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_atributos": "S",
            "pagina": 1,
            "quantidade": 10
        }
        headers = {"accept": "application/json", "token": token}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            yield {"type": "error", "message": f"Falha ao consultar andamentos no SEI (HTTP {response.status_code})"}
            return

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        andamentos_primeira_pagina = data.get("Andamentos", [])

        if total_itens == 0:
            yield {"type": "done", "andamentos": []}
            return

        if total_itens <= 10:
            yield {"type": "done", "andamentos": andamentos_primeira_pagina}
            return

        quantidade_por_pagina = 10
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)
        batch_size = 20
        max_retries = 3

        logger.info(
            f"Andamentos stream fetch: processo={protocolo} total={total_itens} "
            f"paginas={total_paginas} batch_size={batch_size}"
        )

        resultados_por_pagina = {1: andamentos_primeira_pagina}

        # Fetch last page
        if total_paginas > 1:
            ultima_pagina = await buscar_pagina_andamentos(
                token, protocolo, id_unidade, total_paginas, quantidade_por_pagina
            )
            if not isinstance(ultima_pagina, Exception) and ultima_pagina != []:
                resultados_por_pagina[total_paginas] = ultima_pagina

        # Yield initial progress
        items_so_far = sum(len(v) for v in resultados_por_pagina.values())
        yield {"type": "progress", "loaded": items_so_far, "total": total_itens}

        # Middle pages
        paginas_pendentes = [
            p for p in range(2, total_paginas)
            if p not in resultados_por_pagina
        ]
        if total_paginas not in resultados_por_pagina and total_paginas > 1:
            paginas_pendentes.append(total_paginas)

        for tentativa in range(max_retries):
            if not paginas_pendentes:
                break

            if tentativa > 0:
                logger.info(f"Stream retry {tentativa}/{max_retries}: refazendo {len(paginas_pendentes)} páginas falhadas")

            paginas_falhadas = []

            for i in range(0, len(paginas_pendentes), batch_size):
                batch = paginas_pendentes[i:i + batch_size]
                tarefas = [
                    buscar_pagina_andamentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
                    for pagina in batch
                ]

                resultados = await asyncio.gather(*tarefas, return_exceptions=True)

                for j, resultado in enumerate(resultados):
                    pagina = batch[j]
                    if isinstance(resultado, Exception) or resultado == []:
                        paginas_falhadas.append(pagina)
                    else:
                        resultados_por_pagina[pagina] = resultado

                # Yield progress after each batch
                items_so_far = sum(len(v) for v in resultados_por_pagina.values())
                yield {"type": "progress", "loaded": items_so_far, "total": total_itens}

            paginas_pendentes = paginas_falhadas

        if paginas_pendentes:
            yield {
                "type": "error",
                "message": f"Falha ao buscar {len(paginas_pendentes)} páginas de andamentos após {max_retries} tentativas.",
            }
            return

        # Combine in page order
        todas_tarefas = []
        for pagina in sorted(resultados_por_pagina.keys()):
            todas_tarefas.extend(resultados_por_pagina[pagina])

        logger.info(f"Andamentos stream fetch complete: processo={protocolo} total={len(todas_tarefas)}")
        yield {"type": "done", "andamentos": todas_tarefas}

    except Exception as e:
        logger.error(f"Erro no stream de andamentos: {str(e)}", exc_info=True)
        yield {"type": "error", "message": f"Erro ao buscar andamentos: {str(e)}"}


async def login(usuario: str, senha: str, orgao: str, max_tentativas: int = 3):
    """
    Autentica um usuário na API SEI.
    Retorna a resposta bruta (Token, Login, Unidades).
    Retry em erros de conexão/timeout - falha rápida em credenciais inválidas (401).
    """
    url = f"{settings.SEI_BASE_URL}/orgaos/usuarios/login"
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    body = {"Usuario": usuario, "Senha": senha, "Orgao": orgao}
    last_error = None

    for tentativa in range(max_tentativas):
        try:
            response = await http_client.post(url, headers=headers, json=body, timeout=30)

            logger.info(f"SEI login response: status={response.status_code} (tentativa {tentativa + 1}/{max_tentativas})")

            if response.status_code != 200:
                logger.error(
                    f"SEI login FAILED: status={response.status_code} "
                    f"user={usuario} orgao={orgao} "
                    f"response_body={response.text[:2000]}"
                )

            if response.status_code == 401:
                try:
                    data = response.json()
                    message = data.get("Message", "Credenciais inválidas")
                except Exception:
                    message = "Credenciais inválidas"
                raise HTTPException(status_code=401, detail=message)

            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=ErrorDetail(
                        type=ErrorType.EXTERNAL_SERVICE_ERROR,
                        message="Falha ao autenticar no SEI",
                        details={"status_code": response.status_code, "response": response.text}
                    ).dict()
                )

            try:
                return response.json()
            except Exception as e:
                logger.error(f"SEI login retornou resposta não-JSON (status 200): {response.text[:200]}")
                raise HTTPException(
                    status_code=502,
                    detail=ErrorDetail(
                        type=ErrorType.EXTERNAL_SERVICE_ERROR,
                        message="Resposta inválida do serviço SEI",
                        details={"error": "Resposta não é JSON válido"}
                    ).dict()
                )
        except HTTPException:
            raise
        except httpx.RequestError as e:
            last_error = e
            logger.warning(
                f"SEI login conexão falhou tentativa {tentativa + 1}/{max_tentativas} "
                f"user={usuario} orgao={orgao} — {type(e).__name__}: {e}"
            )
            if tentativa < max_tentativas - 1:
                await asyncio.sleep(RETRY_BACKOFF)

    raise HTTPException(
        status_code=500,
        detail=ErrorDetail(
            type=ErrorType.EXTERNAL_SERVICE_ERROR,
            message="Erro ao conectar com o serviço SEI para login",
            details={"error": str(last_error), "tentativas": max_tentativas}
        ).dict()
    )


# Removed `consultar_procedimento` (live SEI lookup of UnidadesProcedimentoAberto).
# Unidades em aberto são derivadas client-side dos andamentos via algoritmo
# canônico — ver `studio/src/lib/process-flow-utils.ts:deriveOpenUnitsFromAndamentos`
# e `api-sei-atividaes/app/models/estoque_rules.py`.



async def consultar_documento(token: str, id_unidade: str, documento_formatado: str):
    try:
        logger.debug(f"Consultando documento: {documento_formatado}")
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos"
        params = {"protocolo_documento": documento_formatado, "sinal_completo": "N"}
        headers = {"accept": "application/json", "token": token}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)
        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao consultar documento no SEI")
        return response.json()
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para consultar documento",
                details={"error": str(e)}
            ).dict()
        )


async def assinar_documento(
    token: str,
    id_unidade: str,
    protocolo_documento: str,
    orgao: str,
    cargo: str,
    id_login: str,
    senha: str,
    id_usuario: str,
) -> dict:
    """
    Assina um documento no SEI.
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos/assinar"
        headers = {"accept": "application/json", "token": token, "Content-Type": "application/json"}
        body = {
            "ProtocoloDocumento": protocolo_documento,
            "Orgao": orgao,
            "Cargo": cargo,
            "IdLogin": id_login,
            "Senha": senha,
            "IdUsuario": id_usuario,
        }

        logger.info(
            f"SEI assinar_documento REQUEST: url={url} "
            f"body={{ProtocoloDocumento={protocolo_documento}, Orgao={orgao}, Cargo={cargo}, "
            f"IdLogin={id_login}, IdUsuario={id_usuario}, Senha=***({len(senha)}chars)}}"
        )

        response = await http_client.patch(url, headers=headers, json=body, timeout=30)

        logger.info(
            f"SEI assinar_documento RESPONSE: status={response.status_code} "
            f"headers={dict(response.headers)} "
            f"body={response.text[:2000]}"
        )

        if response.status_code not in (200, 204):
            logger.error(
                f"SEI assinar_documento FAILED: status={response.status_code} "
                f"protocolo={protocolo_documento} unidade={id_unidade} "
                f"response_body={response.text[:2000]}"
            )
            raise HTTPException(status_code=response.status_code, detail=response.text[:2000])

        # 204 No Content = success with no body
        if response.status_code == 204 or not response.text.strip():
            return {"status": "ok"}

        return response.json()
    except HTTPException:
        raise
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para assinar documento",
                details={"error": str(e)}
            ).dict()
        )


async def baixar_documento(token: str, id_unidade: str, documento_formatado: str, numero_processo: str = None):
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos/baixar"
        headers = {"accept": "application/json", "token": token}
        params = {"protocolo_documento": documento_formatado}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3)

        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao baixar documento do SEI")

        content_disposition = response.headers.get("content-disposition", "")
        match = re.search(r'filename="(.+)"', content_disposition)
        filename = match.group(1) if match else f"documento_{documento_formatado}.html"

        # Detectar tipo de arquivo e retornar estrutura com tipo e conteúdo
        if filename.lower().endswith(".html") or filename.lower().endswith(".htm"):
            try:
                # Converter HTML para Markdown diretamente em memória
                html_content = response.content.decode('utf-8')
                md_content = converte_html_para_markdown_memoria(html_content)
                return {"tipo": "html", "conteudo": md_content, "filename": filename}
            except Exception as e:
                logger.error(f"Falha na conversão HTML->MD: {str(e)}")
                return None
        elif filename.lower().endswith(".pdf"):
            # Para PDF, retornar o conteúdo binário
            return {"tipo": "pdf", "conteudo": response.content, "filename": filename}
        else:
            logger.warning(f"Tipo de arquivo não suportado: {filename}")
            return None
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para baixar documento",
                details={"error": str(e)}
            ).dict()
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar documento baixado",
                details={"error": str(e)}
            ).dict()
        )
