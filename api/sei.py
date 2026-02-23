import re
import httpx
import math
import asyncio
import logging
from fastapi import HTTPException
from .schemas_legacy import ErrorDetail, ErrorType
from .utils import converte_html_para_markdown_memoria
from .config import settings

logger = logging.getLogger(__name__)

# Cliente HTTP global com connection pool
http_client = httpx.AsyncClient(
    timeout=httpx.Timeout(60.0, connect=10.0),
    limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    http2=True
)


async def _fazer_requisicao_com_retry(url: str, headers: dict, params: dict, max_tentativas: int = 3, timeout: int = 60):
    """
    Faz uma requisição HTTP com retry automático em caso de falha de rede.
    Retorna imediatamente para respostas HTTP (inclusive 4xx/5xx).
    Só faz retry em timeout e erros de conexão.
    """
    for tentativa in range(max_tentativas):
        try:
            response = await http_client.get(url, headers=headers, params=params, timeout=timeout)
            if response.status_code >= 400:
                logger.warning(
                    f"HTTP {response.status_code} "
                    f"GET {url} params={params} — body={response.text[:500]}"
                )
            return response
        except httpx.TimeoutException as e:
            logger.warning(
                f"TIMEOUT na tentativa {tentativa + 1}/{max_tentativas} "
                f"GET {url} params={params} — {type(e).__name__}: {e}"
            )
            if tentativa == max_tentativas - 1:
                raise e
        except httpx.ConnectError as e:
            logger.warning(
                f"CONNECT_ERROR na tentativa {tentativa + 1}/{max_tentativas} "
                f"GET {url} params={params} — {type(e).__name__}: {e}"
            )
            if tentativa == max_tentativas - 1:
                raise e
        except httpx.RequestError as e:
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
        headers = {"accept": "application/json", "token": f'"{token}"'}

        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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
    Strategy: first page + last page upfront, then middle pages in batches of 20.
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
        headers = {"accept": "application/json", "token": f'"{token}"'}
        logger.debug(f"Fazendo requisição inicial de documentos para processo: {protocolo}")
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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
        batch_size = 20
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
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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
            response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

            if response.status_code == 200:
                penultima = response.json().get("Andamentos", [])
                combinados = penultima + ultima_pagina_andamentos
                return combinados[-quantidade:]

        return ultima_pagina_andamentos[-quantidade:]
    except Exception as e:
        logger.warning(f"Erro ao buscar últimos andamentos: {str(e)}")
        return []


async def _buscar_pagina_andamentos(token: str, protocolo: str, id_unidade: str, pagina: int, quantidade_por_pagina: int):
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
        headers = {"accept": "application/json", "token": f'"{token}"'}

        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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


async def listar_tarefa_parcial(token: str, protocolo: str, id_unidade: str):
    """
    Fetch first 5 + last 5 pages of andamentos (≤100 items) for fast initial render.
    Returns (andamentos, total_itens, parcial) tuple.
    If total_paginas <= 10, returns all data with parcial=False.
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_atributos": "S",
            "pagina": 1,
            "quantidade": 10
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao consultar andamentos no SEI")

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        andamentos_primeira_pagina = data.get("Andamentos", [])

        if total_itens == 0:
            return [], 0, False

        quantidade_por_pagina = 10
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)

        # Small process: fetch all pages (no benefit to partial)
        if total_paginas <= 10:
            if total_paginas == 1:
                return andamentos_primeira_pagina, total_itens, False

            tarefas = [
                _buscar_pagina_andamentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
                for pagina in range(2, total_paginas + 1)
            ]
            resultados = await asyncio.gather(*tarefas, return_exceptions=True)
            todas = andamentos_primeira_pagina.copy()
            for resultado in resultados:
                if isinstance(resultado, Exception):
                    continue
                todas.extend(resultado)
            return todas, total_itens, False

        # Large process: fetch pages 2-5 + last 5 pages
        logger.info(
            f"Partial fetch: processo={protocolo} total_paginas={total_paginas} "
            f"total_itens={total_itens} — fetching first 5 + last 5 pages"
        )

        first_pages = list(range(2, 6))  # pages 2,3,4,5
        last_pages = list(range(total_paginas - 4, total_paginas + 1))  # last 5 pages

        # Deduplicate in case of overlap (e.g. total_paginas=12)
        all_pages = sorted(set(first_pages + last_pages))

        tarefas = [
            _buscar_pagina_andamentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
            for pagina in all_pages
        ]
        resultados = await asyncio.gather(*tarefas, return_exceptions=True)

        # Combine: page 1 data + first pages data
        primeiros = andamentos_primeira_pagina.copy()
        ultimos = []

        for i, pagina in enumerate(all_pages):
            resultado = resultados[i]
            if isinstance(resultado, Exception):
                continue
            if pagina <= 5:
                primeiros.extend(resultado)
            else:
                ultimos.extend(resultado)

        andamentos = primeiros + ultimos
        logger.info(
            f"Partial fetch complete: processo={protocolo} "
            f"returned {len(andamentos)} items (first {len(primeiros)} + last {len(ultimos)})"
        )
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
    Uses smaller page size (10) for the discovery request to avoid SEI timeouts
    on processes with many documents.
    Returns (documentos, total_itens, parcial) tuple.
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        # Use small page size for discovery to avoid timeout on large processes
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
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao listar documentos no SEI")

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        documentos_primeira_pagina = data.get("Documentos", [])

        if total_itens == 0:
            return [], 0, False

        # Small process: fetch all remaining pages
        if total_itens <= 10:
            return documentos_primeira_pagina, total_itens, False

        quantidade_por_pagina = 10
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)

        if total_paginas <= 10:
            tarefas = [
                _buscar_pagina_documentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
                for pagina in range(2, total_paginas + 1)
            ]
            resultados = await asyncio.gather(*tarefas, return_exceptions=True)
            todos = documentos_primeira_pagina.copy()
            for resultado in resultados:
                if isinstance(resultado, Exception):
                    continue
                todos.extend(resultado)
            return todos, total_itens, False

        # Large: fetch first page (already have) + last page
        logger.info(
            f"Partial docs fetch: processo={protocolo} total_paginas={total_paginas} "
            f"total_itens={total_itens} — fetching first + last page (qty={quantidade_por_pagina})"
        )

        ultima_pagina_docs = await _buscar_pagina_documentos(
            token, protocolo, id_unidade, total_paginas, quantidade_por_pagina
        )

        documentos = documentos_primeira_pagina + ultima_pagina_docs
        logger.info(
            f"Partial docs fetch complete: processo={protocolo} "
            f"returned {len(documentos)} items"
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
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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
            ultima_pagina = await _buscar_pagina_andamentos(
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
                    _buscar_pagina_andamentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
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


async def login(usuario: str, senha: str, orgao: str):
    """
    Autentica um usuário na API SEI.
    Retorna a resposta bruta (Token, Login, Unidades).
    Sem retry - falha rápida em credenciais inválidas.
    """
    try:
        url = f"{settings.SEI_BASE_URL}/orgaos/usuarios/login"
        headers = {"accept": "application/json", "Content-Type": "application/json"}
        body = {"Usuario": usuario, "Senha": senha, "Orgao": orgao}

        response = await http_client.post(url, headers=headers, json=body, timeout=30)

        logger.info(
            f"SEI login response: status={response.status_code} "
            f"headers={dict(response.headers)} "
            f"body={response.text[:1000]}"
        )

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
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para login",
                details={"error": str(e)}
            ).dict()
        )


async def consultar_procedimento(token: str, protocolo: str, id_unidade: str):
    """
    Consulta informações de um procedimento (processo) no SEI.
    Retorna dados incluindo UnidadesProcedimentoAberto e LinkAcesso.
    """
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/consulta"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_unidades_procedimento_aberto": "S",
            "sinal_completo": "N",
            "sinal_assuntos": "N",
            "sinal_interessados": "N",
            "sinal_observacoes": "N",
            "sinal_andamento_geracao": "N",
            "sinal_andamento_conclusao": "N",
            "sinal_ultimo_andamento": "N",
            "sinal_procedimentos_relacionados": "N",
            "sinal_procedimentos_anexados": "N",
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}

        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

        if response.status_code != 200:
            _raise_sei_error(response, "Falha ao consultar procedimento no SEI")

        return response.json()
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para consultar procedimento",
                details={"error": str(e)}
            ).dict()
        )


async def verificar_saude():
    """
    Verifica se a API SEI está online.
    Retorna {online: bool, status_code: int}.
    """
    try:
        url = f"{settings.SEI_BASE_URL}/orgaos"
        params = {"pagina": 1, "quantidade": 10}
        headers = {"accept": "application/json"}

        response = await http_client.get(url, headers=headers, params=params, timeout=10)

        return {"online": response.status_code == 200, "status_code": response.status_code}
    except Exception as e:
        logger.warning(f"Health check SEI falhou: {str(e)}")
        return {"online": False, "status_code": 0}


async def consultar_documento(token: str, id_unidade: str, documento_formatado: str):
    try:
        logger.debug(f"Consultando documento: {documento_formatado}")
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos"
        params = {"protocolo_documento": documento_formatado, "sinal_completo": "N"}
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)
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
        headers = {"accept": "application/json", "token": f'"{token}"', "Content-Type": "application/json"}
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
        headers = {"accept": "application/json", "token": f'"{token}"'}
        params = {"protocolo_documento": documento_formatado}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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
