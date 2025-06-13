import re
import requests
from fastapi import HTTPException
from .models import Processo, ErrorDetail, ErrorType
from .utils import converte_documentos_para_markdown
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

def baixar_documento(token: str, id_unidade: str, documento_formatado: str):
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

        with open(filename, "wb") as f:
            f.write(response.content)

        if filename.lower().endswith(".html") or filename.lower().endswith(".htm"):
            caminho_md = converte_documentos_para_markdown(filename)
            return caminho_md
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
    
