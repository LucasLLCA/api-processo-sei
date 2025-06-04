import re
import requests
from fastapi import HTTPException
from .db import get_db_connection
from .models import Processo, ErrorDetail, ErrorType
from .config import SEI_CREDENTIALS
from functools import lru_cache
from .utils import converte_documentos_para_markdown

BASE_URL = "https://api.sead.pi.gov.br/sei/v1"

@lru_cache(maxsize=1)
def obter_token():
    try:
        response = requests.post(f"{BASE_URL}/orgaos/usuarios/login", json={
            "Usuario": SEI_CREDENTIALS["usuario"],
            "Senha": SEI_CREDENTIALS["senha"],
            "Orgao": SEI_CREDENTIALS["orgao"]
        })
        if response.status_code != 200:
            raise HTTPException(
                status_code=500,
                detail=ErrorDetail(
                    type=ErrorType.AUTHENTICATION_ERROR,
                    message="Falha ao autenticar no SEI",
                    details={"status_code": response.status_code, "response": response.text}
                ).dict()
            )
        return response.json()["Token"]
    except requests.RequestException as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.EXTERNAL_SERVICE_ERROR,
                message="Erro ao conectar com o serviço SEI",
                details={"error": str(e)}
            ).dict()
        )

def buscar_processo(numero: str) -> Processo:
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        numero_limpo = re.sub(r'[./-]', '', numero)

        query = """
            SELECT protocol, protocol, sector_id::text, type_name
            FROM painel_sead_prod.public.protocol
            WHERE REPLACE(REPLACE(REPLACE(protocol, '.', ''), '/', ''), '-', '') = %s
            LIMIT 1
        """
        cur.execute(query, (numero_limpo,))
        row = cur.fetchone()
        conn.close()

        if not row:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Processo não encontrado",
                    details={"numero_processo": numero}
                ).dict()
            )
        
        return Processo(numero=row[0], protocolo=row[1], id_unidade=row[2], assunto=row[3])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.DATABASE_ERROR,
                message="Erro ao buscar processo no banco de dados",
                details={"error": str(e), "numero_processo": numero}
            ).dict()
        )

def listar_documentos(token, protocolo, id_unidade):
    try:
        url = f"{BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
        params = {
            "protocolo_procedimento": protocolo,
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

def listar_tarefa(token, protocolo, id_unidade):
    try:
        url = f"{BASE_URL}/unidades/{id_unidade}/procedimentos/andamentos"
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

def consultar_documento(token, id_unidade, documento_formatado):
    try:
        url = f"{BASE_URL}/unidades/{id_unidade}/documentos"
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

def baixar_documento(token, id_unidade, documento_formatado):
    try:
        url = f"{BASE_URL}/unidades/{id_unidade}/documentos/baixar"
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
    