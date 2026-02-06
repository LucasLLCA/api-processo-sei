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


async def _fazer_requisicao_com_retry(url: str, headers: dict, params: dict, max_tentativas: int = 3, timeout: int = 30):
    """
    Faz uma requisição HTTP com retry automático em caso de falha
    """
    for tentativa in range(max_tentativas):
        try:
            response = await http_client.get(url, headers=headers, params=params, timeout=timeout)
            return response
        except (httpx.TimeoutException, httpx.ConnectError) as e:
            if tentativa == max_tentativas - 1:
                raise e

            # Backoff exponencial: 1s, 2s, 4s
            tempo_espera = 2 ** tentativa
            logger.debug(f"Tentativa {tentativa + 1} falhou, aguardando {tempo_espera}s: {str(e)}")
            await asyncio.sleep(tempo_espera)
        except httpx.RequestError as e:
            # Para outros erros, falha imediatamente
            raise e


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

        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

        if response.status_code == 504:
            logger.warning(f"Timeout na página {pagina}, pulando esta página")
            return []

        if response.status_code != 200:
            logger.warning(f"Erro na página {pagina} (status {response.status_code}), pulando esta página")
            return []

        return response.json().get("Documentos", [])
    except (httpx.TimeoutException, httpx.ConnectError) as e:
        logger.warning(f"Timeout/conexão falhou na página {pagina}, pulando: {str(e)}")
        return []
    except httpx.RequestError as e:
        logger.warning(f"Erro de requisição na página {pagina}, pulando: {str(e)}")
        return []
    except Exception as e:
        logger.warning(f"Erro inesperado na página {pagina}, pulando: {str(e)}")
        return []


async def listar_documentos(token: str, protocolo: str, id_unidade: str):
    try:
        # Primeira requisição para descobrir o total de itens
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
            "pagina": 1,
            "quantidade": 50,  # Já busca 50 na primeira requisição
            "sinal_geracao": "N",
            "sinal_assinaturas": "N",
            "sinal_publicacao": "N",
            "sinal_campos": "N",
            "sinal_completo": "S"
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}
        logger.debug(f"Fazendo requisição inicial para processo: {protocolo}")
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao listar documentos no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        documentos_primeira_pagina = data.get("Documentos", [])

        if total_itens == 0:
            return []

        # Se total é pequeno, já temos todos os documentos
        if total_itens <= 50:
            return documentos_primeira_pagina

        # Para totais maiores, usar paginação paralela
        quantidade_por_pagina = 50
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)

        logger.debug(f"Total de documentos: {total_itens}, Páginas: {total_paginas}")

        # Executar requisições em paralelo (exceto a primeira que já temos)
        tarefas = [
            _buscar_pagina_documentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
            for pagina in range(2, total_paginas + 1)
        ]

        resultados = await asyncio.gather(*tarefas, return_exceptions=True)

        # Combinar resultados
        todos_documentos = documentos_primeira_pagina.copy()
        for i, resultado in enumerate(resultados):
            if isinstance(resultado, Exception):
                logger.error(f"Falha na página {i + 2}: {str(resultado)}")
                continue
            todos_documentos.extend(resultado)
            logger.debug(f"Página {i + 2} carregada: {len(resultado)} documentos")

        logger.debug(f"Total de documentos carregados: {len(todos_documentos)}")

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
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao listar documentos no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )

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

        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

        if response.status_code == 504:
            logger.warning(f"Timeout na página {pagina} de andamentos, pulando esta página")
            return []

        if response.status_code != 200:
            logger.warning(f"Erro na página {pagina} de andamentos (status {response.status_code}), pulando esta página")
            return []

        return response.json().get("Andamentos", [])
    except (httpx.TimeoutException, httpx.ConnectError) as e:
        logger.warning(f"Timeout/conexão falhou na página {pagina} de andamentos, pulando: {str(e)}")
        return []
    except httpx.RequestError as e:
        logger.warning(f"Erro de requisição na página {pagina} de andamentos, pulando: {str(e)}")
        return []
    except Exception as e:
        logger.warning(f"Erro inesperado na página {pagina} de andamentos, pulando: {str(e)}")
        return []


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
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao consultar andamentos no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )

        data = response.json()
        total_itens = data.get("Info", {}).get("TotalItens", 0)
        andamentos_primeira_pagina = data.get("Andamentos", [])

        if total_itens == 0:
            return []

        # Se total é pequeno, já temos todos os andamentos
        if total_itens <= 10:
            return andamentos_primeira_pagina

        # Para totais maiores, usar paginação paralela
        quantidade_por_pagina = 10
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)

        logger.debug(f"Total de andamentos: {total_itens}, Páginas: {total_paginas}")

        # Executar requisições em paralelo
        tarefas = [
            _buscar_pagina_andamentos(token, protocolo, id_unidade, pagina, quantidade_por_pagina)
            for pagina in range(2, total_paginas + 1)
        ]

        resultados = await asyncio.gather(*tarefas, return_exceptions=True)

        # Combinar resultados
        todas_tarefas = andamentos_primeira_pagina.copy()
        for i, resultado in enumerate(resultados):
            if isinstance(resultado, Exception):
                logger.error(f"Falha na página {i + 2}: {str(resultado)}")
                continue
            todas_tarefas.extend(resultado)
            logger.debug(f"Página {i + 2}/{total_paginas} carregada: {len(resultado)} andamentos")

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


async def consultar_documento(token: str, id_unidade: str, documento_formatado: str):
    try:
        logger.debug(f"Consultando documento: {documento_formatado}")
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos"
        params = {"protocolo_documento": documento_formatado, "sinal_completo": "N"}
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao consultar documento no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )
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


async def baixar_documento(token: str, id_unidade: str, documento_formatado: str, numero_processo: str = None):
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos/baixar"
        headers = {"accept": "application/json", "token": f'"{token}"'}
        params = {"protocolo_documento": documento_formatado}
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao baixar documento do SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )

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
