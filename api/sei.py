import re
import requests
from fastapi import HTTPException
from .db import get_db_connection
from .models import Processo
from .config import SEI_CREDENTIALS
from functools import lru_cache
from .utils import converte_documentos_para_markdown

BASE_URL = "https://api.sead.pi.gov.br/sei/v1"

@lru_cache(maxsize=1)
def obter_token():
    response = requests.post(f"{BASE_URL}/orgaos/usuarios/login", json={
        "Usuario": SEI_CREDENTIALS["usuario"],
        "Senha": SEI_CREDENTIALS["senha"],
        "Orgao": SEI_CREDENTIALS["orgao"]
    })
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Falha ao autenticar no SEI")
    return response.json()["Token"]

def buscar_processo(numero: str) -> Processo:
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
        raise HTTPException(status_code=404, detail="Processo não encontrado")
    
    return Processo(numero=row[0], protocolo=row[1], id_unidade=row[2], assunto=row[3])

def listar_documentos(token, protocolo, id_unidade):
    url = f"{BASE_URL}/unidades/{id_unidade}/procedimentos/documentos"
    params = {
        "protocolo_procedimento": protocolo,
        "pagina": 1,
        "quantidade": 10
    }
    headers = {"accept": "application/json", "token": f'"{token}"'}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Falha ao listar documentos")
    return response.json().get("Documentos", [])

def listar_tarefa(token, protocolo, id_unidade):
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
        raise HTTPException(status_code=500, detail="Falha ao consultar documento")
    return response.json().get("Andamentos", [])

def consultar_documento(token, id_unidade, documento_formatado):
    url = f"{BASE_URL}/unidades/{id_unidade}/documentos"
    params = {"protocolo_documento": documento_formatado, "sinal_completo": "N"}
    headers = {"accept": "application/json", "token": f'"{token}"'}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        raise HTTPException(status_code=500, detail="Falha ao consultar documento")
    return response.json()

def baixar_documento(token, id_unidade, documento_formatado):
    url = f"{BASE_URL}/unidades/{id_unidade}/documentos/baixar"
    headers = {"accept": "application/json", "token": f'"{token}"'}
    params = {"protocolo_documento": documento_formatado}
    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        print("Erro ao baixar documento:", response.status_code)
        return None

    content_disposition = response.headers.get("content-disposition", "")
    match = re.search(r'filename="(.+)"', content_disposition)
    filename = match.group(1) if match else f"documento_{documento_formatado}.html"

    with open(filename, "wb") as f:
        f.write(response.content)

    print("Arquivo baixado:", filename)

    if filename.lower().endswith(".html") or filename.lower().endswith(".htm"):
        caminho_md = converte_documentos_para_markdown(filename)
        print("Markdown gerado:", caminho_md)
        return caminho_md
    else:
        print("Arquivo não é HTML, não convertido:", filename)
        return None
    