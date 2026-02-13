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
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

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
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

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
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

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
        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

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
            response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

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

        response = await _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)

        if response.status_code != 200:
            error_message = "Falha ao consultar procedimento no SEI"
            try:
                error_data = response.json()
                if isinstance(error_data, dict) and "detail" in error_data:
                    detail_list = error_data["detail"]
                    if isinstance(detail_list, list) and len(detail_list) > 0:
                        error_message = detail_list[0].get("msg", error_message)
            except Exception:
                pass

            raise HTTPException(
                status_code=response.status_code,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message=error_message,
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )

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
