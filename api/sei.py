import re
import requests
import io
import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from fastapi import HTTPException
from minio import Minio
from minio.error import S3Error
from .models import ErrorDetail, ErrorType
from .utils import converte_html_para_markdown_memoria
from .config import settings

def _fazer_requisicao_com_retry(url: str, headers: dict, params: dict, max_tentativas: int = 3, timeout: int = 30):
    """
    Faz uma requisição HTTP com retry automático em caso de falha
    """
    for tentativa in range(max_tentativas):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if tentativa == max_tentativas - 1:
                raise e
            
            # Backoff exponencial: 1s, 2s, 4s
            tempo_espera = 2 ** tentativa
            print(f"[DEBUG] Tentativa {tentativa + 1} falhou, aguardando {tempo_espera}s antes de tentar novamente: {str(e)}")
            time.sleep(tempo_espera)
        except requests.exceptions.RequestException as e:
            # Para outros erros, falha imediatamente
            raise e

def _buscar_pagina_documentos(token: str, protocolo: str, id_unidade: str, pagina: int, quantidade_por_pagina: int):
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
        
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
        
        if response.status_code == 504:
            print(f"[WARN] Timeout na página {pagina}, pulando esta página")
            return []
        
        if response.status_code != 200:
            print(f"[WARN] Erro na página {pagina} (status {response.status_code}), pulando esta página")
            return []
        
        return response.json().get("Documentos", [])
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        print(f"[WARN] Timeout/conexão falhou na página {pagina}, pulando: {str(e)}")
        return []
    except requests.RequestException as e:
        print(f"[WARN] Erro de requisição na página {pagina}, pulando: {str(e)}")
        return []
    except Exception as e:
        print(f"[WARN] Erro inesperado na página {pagina}, pulando: {str(e)}")
        return []

def listar_documentos(token: str, protocolo: str, id_unidade: str):
    try:
        # Primeira requisição para descobrir o total de itens
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
        print(f"[DEBUG] Fazendo requisição inicial para processo: {protocolo}")
        print(f"[DEBUG] Parâmetros: {params}")
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao listar documentos no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )
        
        total_itens = response.json().get("Info", {}).get("TotalItens", 0)
        
        if total_itens == 0:
            return []
        
        # Se total é pequeno, fazer uma única requisição
        if total_itens <= 50:
            params["quantidade"] = total_itens
            response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=500,
                    detail=ErrorDetail(
                        type=ErrorType.EXTERNAL_SERVICE_ERROR,
                        message="Falha ao listar documentos no SEI",
                        details={"status_code": response.status_code, "response": response.text}
                    ).dict()
                )
            return response.json().get("Documentos", [])
        
        # Para totais maiores, usar paginação paralela
        quantidade_por_pagina = 50  # Tamanho otimizado por página
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)
        
        print(f"[DEBUG] Total de documentos: {total_itens}, Páginas: {total_paginas}")
        
        # Executar requisições em paralelo
        todos_documentos = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submeter todas as páginas para execução paralela
            futures = {
                executor.submit(_buscar_pagina_documentos, token, protocolo, id_unidade, pagina, quantidade_por_pagina): pagina
                for pagina in range(1, total_paginas + 1)
            }
            
            # Coletar resultados conforme completam
            for future in as_completed(futures):
                pagina = futures[future]
                try:
                    documentos_pagina = future.result()
                    todos_documentos.extend(documentos_pagina)
                    print(f"[DEBUG] Página {pagina} carregada: {len(documentos_pagina)} documentos")
                except Exception as e:
                    print(f"[ERRO] Falha na página {pagina}: {str(e)}")
                    # Continua com outras páginas mesmo se uma falhar
        
        print(f"[DEBUG] Total de documentos carregados: {len(todos_documentos)}")
        
        # Log dos primeiros documentos para debug
        if todos_documentos:
            print(f"[DEBUG] Exemplo de documento carregado: {todos_documentos[0]}")
            if 'DocumentoFormatado' in todos_documentos[0]:
                print(f"[DEBUG] DocumentoFormatado do primeiro documento: {todos_documentos[0]['DocumentoFormatado']}")
        
        return todos_documentos
    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para listar documentos",
                details={"error": str(e)}
            ).dict()
        )

def _buscar_pagina_andamentos(token: str, protocolo: str, id_unidade: str, pagina: int, quantidade_por_pagina: int):
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
        
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
        
        if response.status_code == 504:
            print(f"[WARN] Timeout na página {pagina} de andamentos, pulando esta página")
            return []
        
        if response.status_code != 200:
            print(f"[WARN] Erro na página {pagina} de andamentos (status {response.status_code}), pulando esta página")
            return []
        
        return response.json().get("Andamentos", [])
    except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
        print(f"[WARN] Timeout/conexão falhou na página {pagina} de andamentos, pulando: {str(e)}")
        return []
    except requests.RequestException as e:
        print(f"[WARN] Erro de requisição na página {pagina} de andamentos, pulando: {str(e)}")
        return []
    except Exception as e:
        print(f"[WARN] Erro inesperado na página {pagina} de andamentos, pulando: {str(e)}")
        return []

def listar_tarefa(token: str, protocolo: str, id_unidade: str):
    try:
        # Primeira requisição para descobrir o total de itens
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_atributos": "S",
            "pagina": 1,
            "quantidade": 1
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.EXTERNAL_SERVICE_ERROR,
                    message="Falha ao consultar andamentos no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )
        
        total_itens = response.json().get("Info", {}).get("TotalItens", 0)
        
        if total_itens == 0:
            return []
        
        # Se total é pequeno, fazer uma única requisição
        if total_itens <= 10:
            params["quantidade"] = total_itens
            response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=500,
                    detail=ErrorDetail(
                        type=ErrorType.EXTERNAL_SERVICE_ERROR,
                        message="Falha ao consultar andamentos no SEI",
                        details={"status_code": response.status_code, "response": response.text}
                    ).dict()
                )
            return response.json().get("Andamentos", [])
        
        # Para totais maiores, usar paginação paralela
        quantidade_por_pagina = 10  # Quantidade fixa de itens por página para andamentos
        total_paginas = math.ceil(total_itens / quantidade_por_pagina)
        
        print(f"[DEBUG] Total de andamentos: {total_itens}, Páginas: {total_paginas}, Itens por página: {quantidade_por_pagina}")
        
        # Executar requisições em paralelo
        todas_tarefas = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submeter todas as páginas para execução paralela
            futures = {
                executor.submit(_buscar_pagina_andamentos, token, protocolo, id_unidade, pagina, quantidade_por_pagina): pagina
                for pagina in range(1, total_paginas + 1)
            }
            
            # Coletar resultados conforme completam
            itens_coletados = 0
            for future in as_completed(futures):
                pagina = futures[future]
                try:
                    tarefas_pagina = future.result()
                    todas_tarefas.extend(tarefas_pagina)
                    itens_coletados += len(tarefas_pagina)
                    print(f"[DEBUG] Página {pagina}/{total_paginas} carregada: {len(tarefas_pagina)} andamentos (Total coletado: {itens_coletados}/{total_itens})")
                except Exception as e:
                    print(f"[ERRO] Falha na página {pagina}: {str(e)}")
                    # Continua com outras páginas mesmo se uma falhar
        
        print(f"[DEBUG] Paginação concluída. Total esperado: {total_itens}, Total coletado: {len(todas_tarefas)}")
        return todas_tarefas
    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para consultar andamentos",
                details={"error": str(e)}
            ).dict()
        )

def consultar_documento(token: str, id_unidade: str, documento_formatado: str):
    try:
        print(f"[DEBUG] Consultando documento: {documento_formatado}")
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos"
        params = {"protocolo_documento": documento_formatado, "sinal_completo": "N"}
        headers = {"accept": "application/json", "token": f'"{token}"'}
        print(f"[DEBUG] URL: {url}")
        print(f"[DEBUG] Params: {params}")
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=45)
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
    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para consultar documento",
                details={"error": str(e)}
            ).dict()
        )

def baixar_documento(token: str, id_unidade: str, documento_formatado: str, numero_processo: str = None):
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos/baixar"
        headers = {"accept": "application/json", "token": f'"{token}"'}
        params = {"protocolo_documento": documento_formatado}
        response = _fazer_requisicao_com_retry(url, headers, params, max_tentativas=3, timeout=60)

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

        # Inicializar cliente MinIO
        minio_client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY
        )

        # Definir estrutura de pastas no MinIO
        processo_folder = numero_processo if numero_processo else "sem_processo"
        html_object_name = f"{processo_folder}/{filename}"
        
        # Verificar se o arquivo HTML já existe no MinIO
        try:
            minio_client.stat_object(settings.MINIO_BUCKET, html_object_name)
            print(f"[DEBUG] Arquivo HTML já existe no MinIO: {html_object_name}")
            html_exists = True
        except S3Error:
            html_exists = False
        except Exception as e:
            print(f"[WARN] MinIO inacessível (verificação HTML): {str(e)}")
            html_exists = False

        # Se o arquivo HTML não existe, salvar no MinIO
        if not html_exists:
            try:
                html_data = io.BytesIO(response.content)
                minio_client.put_object(
                    settings.MINIO_BUCKET,
                    html_object_name,
                    html_data,
                    len(response.content),
                    content_type="text/html"
                )
                print(f"[DEBUG] Arquivo HTML salvo no MinIO: {html_object_name}")
            except S3Error as e:
                print(f"[WARN] Falha ao salvar HTML no MinIO: {str(e)}")
                # Continua sem salvar no MinIO
            except Exception as e:
                print(f"[WARN] MinIO inacessível (salvamento HTML): {str(e)}")
                # Continua sem salvar no MinIO

        # Processar documento para Markdown se for HTML
        if filename.lower().endswith(".html") or filename.lower().endswith(".htm"):
            md_filename = filename.rsplit('.', 1)[0] + '.md'
            md_object_name = f"{processo_folder}/{md_filename}"
            
            # Verificar se o arquivo MD já existe no MinIO
            try:
                minio_client.stat_object(settings.MINIO_BUCKET, md_object_name)
                print(f"[DEBUG] Arquivo MD já existe no MinIO: {md_object_name}")
                # Retornar o caminho do objeto no MinIO
                return md_object_name
            except S3Error:
                # Arquivo MD não existe, precisa converter e salvar
                pass
            except Exception as e:
                print(f"[WARN] MinIO inacessível (verificação MD): {str(e)}")
                # Continua para conversão local sem MinIO
                pass
            
            # Se chegou aqui, o arquivo MD não existe no MinIO ou MinIO está inacessível
            try:
                # Converter HTML para Markdown diretamente em memória
                html_content = response.content.decode('utf-8')
                md_content = converte_html_para_markdown_memoria(html_content)
                
                # Tentar salvar o MD no MinIO
                try:
                    md_data = io.BytesIO(md_content.encode('utf-8'))
                    minio_client.put_object(
                        settings.MINIO_BUCKET,
                        md_object_name,
                        md_data,
                        len(md_content.encode('utf-8')),
                        content_type="text/markdown"
                    )
                    print(f"[DEBUG] Arquivo MD convertido e salvo no MinIO: {md_object_name}")
                    # Retornar o caminho do objeto no MinIO
                    return md_object_name
                except Exception as minio_error:
                    print(f"[WARN] MinIO inacessível (salvamento MD): {str(minio_error)}")
                    # Retorna o conteúdo convertido em memória
                    return md_content
            except Exception as e:
                print(f"[ERRO] Falha na conversão HTML->MD: {str(e)}")
                return None
        else:
            return None
    except requests.RequestException as e:
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
