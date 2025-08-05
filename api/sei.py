import re
import requests
import io
from fastapi import HTTPException
from minio import Minio
from minio.error import S3Error
from .models import Processo, ErrorDetail, ErrorType
from .utils import converte_html_para_markdown_memoria
from .config import settings

def listar_documentos(token: str, protocolo: str, id_unidade: str):
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
            "pagina": 1,
            "quantidade": 1
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = requests.get(url, headers=headers, params=params)
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
        params["quantidade"] = total_itens
        response = requests.get(url, headers=headers, params=params)
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
    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI para listar documentos",
                details={"error": str(e)}
            ).dict()
        )

def listar_tarefa(token: str, protocolo: str, id_unidade: str):
    try:
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
        params = {
            "protocolo_procedimento": protocolo,
            "sinal_atributos": "N",
            "pagina": 1,
            "quantidade": 10
        }
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = requests.get(url, headers=headers, params=params)
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
        url = f"{settings.SEI_BASE_URL}/unidades/{id_unidade}/documentos"
        params = {"protocolo_documento": documento_formatado, "sinal_completo": "N"}
        headers = {"accept": "application/json", "token": f'"{token}"'}
        response = requests.get(url, headers=headers, params=params)
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
        response = requests.get(url, headers=headers, params=params)

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
            secret_key=settings.MINIO_SECRET_KEY,
            secure=True
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
                print(f"[ERRO] Falha ao salvar HTML no MinIO: {str(e)}")
                raise HTTPException(
                    status_code=500,
                    detail=ErrorDetail(
                        type=ErrorType.PROCESSING_ERROR,
                        message="Erro ao salvar documento HTML no MinIO",
                        details={"error": str(e)}
                    ).dict()
                )

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
                try:
                    # Converter HTML para Markdown diretamente em memória
                    html_content = response.content.decode('utf-8')
                    md_content = converte_html_para_markdown_memoria(html_content)
                    
                    # Salvar o MD no MinIO
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
