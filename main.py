import os
import re
import logging

import psycopg2
import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bs4 import BeautifulSoup
from openai import Client

load_dotenv()
logging.basicConfig(level=logging.INFO)

app = FastAPI()

class Processo(BaseModel):
    numero: str
    protocolo: str
    id_unidade: str
    assunto: str

class Documento(BaseModel):
    documento_formatado: str

class DocumentoDetalhado(BaseModel):
    conteudo: str
    titulo: str

class Retorno(BaseModel):
    status: str

    resumo: dict | None = None
    andamento: dict | None = None

class Andamentos(BaseModel):
    andamento: str

# Database
def get_db_connection():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dbname=os.getenv("DB_NAME")
    )
    return conn

def obter_token():
    payload = {
        "Usuario": os.getenv("SEAD_USUARIO"),
        "Senha": os.getenv("SEAD_SENHA"),
        "Orgao": os.getenv("SEAD_ORGAO")
    }
    response = requests.post("https://api.sead.pi.gov.br/sei/v1/orgaos/usuarios/login", json=payload)
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
    
    return Processo(
        numero=row[0],
        protocolo=row[1],
        id_unidade=row[2],
        assunto=row[3]
    )

def listar_documentos(token: str, protocolo: str, id_unidade: str):
    url = f"https://api.sead.pi.gov.br/sei/v1/unidades/{id_unidade}/procedimentos/documentos"
    params = {
        "protocolo_procedimento": protocolo,
        "pagina": 1,
        "quantidade": 10,
        "sinal_geracao": "N",
        "sinal_assinaturas": "N",
        "sinal_publicacao": "N",
        "sinal_campos": "N",
        "sinal_completo": "N",
    }
    headers = {
        "accept": "application/json",
        "token": f'"{token}"'
    }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        logging.info(response.text)
        raise HTTPException(status_code=500, detail="Falha ao listar documentos")

    return response.json().get("Documentos", [])


def consultar_documento(token: str, id_unidade: str, documento_formatado: str):
    url = f"https://api.sead.pi.gov.br/sei/v1/unidades/{id_unidade}/documentos"
    params = {
        "protocolo_documento": documento_formatado,
        "sinal_completo": "N" 
    }
    headers = {
        "accept": "application/json",
        "token": f'"{token}"'  
    }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code != 200:
        logging.info(response.text)
        raise HTTPException(status_code=500, detail="Falha ao consultar documento")

    return response.json()

def listar_tarefa(token: str, protocolo: str, id_unidade: str) :
    url = f"https://api.sead.pi.gov.br/sei/v1/unidades/{id_unidade}/procedimentos/andamentos"
    params = {
        "protocolo_procedimento": protocolo,
        "sinal_atributos": "N",
        "pagina": 1,
        "quantidade": 10,
    }
    headers = {
        "accept": "application/json",
        "token": f'"{token}"'
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        logging.info(response.text)
        raise HTTPException(status_code=500, detail="Falha ao consultar documento")
    return response.json().get("Andamentos", [])

def converte_documentos_para_markdown(caminho_arquivo: str):

    if not os.path.isfile(caminho_arquivo):
        logging.info(f"Arquivo não encontrado: {caminho_arquivo}")
        return

    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        conteudo = f.read()

    # Remove tags HTML básicas (versão simplificada)
    texto_limpo = re.sub(r'<[^>]+>', '', conteudo)

    # Define novo nome com extensão .md
    base, _ = os.path.splitext(caminho_arquivo)
    caminho_md = base + ".md"

    with open(caminho_md, "w", encoding="utf-8") as f:
        f.write(texto_limpo)

    logging.info(f"Arquivo convertido para Markdown: {caminho_md}")
    return caminho_md

def baixar_documento(token: str, id_unidade: str, documento_formatado: str):
    url = f"https://api.sead.pi.gov.br/sei/v1/unidades/{id_unidade}/documentos/baixar"
    params = {
        "protocolo_documento": documento_formatado,
    }
    headers = {
        "accept": "application/json",
        "token": f'"{token}"'
    }
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        logging.info("Erro ao baixar arquivo:", response.status_code, response.text)
        return None

    content_disposition = response.headers.get("content-disposition", "")
    match = re.search(r'filename="(.+)"', content_disposition)
    filename = match.group(1) if match else "arquivo_baixado.html"

    with open(filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

    logging.info(f"Arquivo salvo como: {filename}")

    if filename.lower().endswith(".html") or filename.lower().endswith(".htm"):
        caminho_md = converte_documentos_para_markdown(filename)
        logging.info(f"Markdown gerado: {caminho_md}")
        return caminho_md

    return None

def enviar_para_ia(caminho_arquivo_md: str) -> dict:
    if not os.path.isfile(caminho_arquivo_md):
        raise HTTPException(status_code=400, detail=f"Arquivo não encontrado: {caminho_arquivo_md}")

    with open(caminho_arquivo_md, "r", encoding="utf-8") as f:
        conteudo = f.read()

    client = Client(
        api_key=os.getenv("OPENAI_API_KEY"),
        base_url="http://plataforma.sobdemanda.mandu.piaui.pro/v1"
    )

    try:
        resposta = client.chat.completions.create(
            model="Qwen3-30B-A3B", 
            messages=[
                {"role": "system", "content": "Você é um assistente juridico que lê documentos Markdown e responde perguntas e resume seu conteúdo."},
                {"role": "user", "content": f"Leia o documento abaixo e me diga do que se trata:\n\n{conteudo}"}
            ],
            temperature=0.7,
        )
        texto_resposta = resposta.choices[0].message.content.strip()

        return {
            "status": "ok",
            "resposta_ia": texto_resposta
        }

    except Exception as e:
        logging.info("Erro ao enviar para IA:", e)
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")

    


@app.get("/andamento/{numero_processo}")
def andamento(numero_processo: str):
    try:
        logging.info("Recebendo requisição para número:", numero_processo)
        token = obter_token()
        logging.info("Token obtido:", token)
        processo = buscar_processo(numero_processo)
        logging.info("Processo encontrado:", processo)
        documentos = listar_documentos(token, processo.protocolo, processo.id_unidade)
        logging.info("Documentos retornados:", documentos)
        andamentos = listar_tarefa(token, processo.protocolo, processo.id_unidade)

        if not documentos:
            raise HTTPException(status_code=404, detail="Nenhum documento encontrado")

        ultimo = documentos[-1]
        ultimo_andamento = andamentos[-1]
        usuario = ultimo_andamento.get("Usuario", {})

        logging.info("Último documento:", ultimo)

        doc_ultimo = consultar_documento(token, processo.id_unidade, ultimo["DocumentoFormatado"])

        caminho_md_ultimo = baixar_documento(token, processo.id_unidade, ultimo["DocumentoFormatado"])

        resposta_ia_ultimo = enviar_para_ia(caminho_md_ultimo) if caminho_md_ultimo else {}

        logging.info("Consulta último OK:", doc_ultimo)

        return {
            "status": "ok",
            "andamento": doc_ultimo,
            "resumo_ultimo": resposta_ia_ultimo,
            "usuario_ultimo_andamento": usuario
        }

    except Exception as e:
        logging.info("Erro geral:", e)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/resumo/{numero_processo}")
def resumo(numero_processo: str):
    try:
        token = obter_token()
        processo = buscar_processo(numero_processo)
        documentos = listar_documentos(token, processo.protocolo, processo.id_unidade)
        andamentos = listar_tarefa(token, processo.protocolo, processo.id_unidade)

        if not documentos:
            raise HTTPException(status_code=404, detail="Nenhum documento encontrado")

        primeiro = documentos[0]
        ultimo = documentos[-1]
        ultimo_andamento = andamentos[-1]
        usuario = ultimo_andamento.get("Usuario", {})

        doc_primeiro = consultar_documento(token, processo.id_unidade, primeiro["DocumentoFormatado"])
        doc_ultimo = consultar_documento(token, processo.id_unidade, ultimo["DocumentoFormatado"])

        caminho_md_primeiro = baixar_documento(token, processo.id_unidade, primeiro["DocumentoFormatado"])
        caminho_md_ultimo = baixar_documento(token, processo.id_unidade, ultimo["DocumentoFormatado"])

        resposta_ia_primeiro = enviar_para_ia(caminho_md_primeiro) if caminho_md_primeiro else {}
        resposta_ia_ultimo = enviar_para_ia(caminho_md_ultimo) if caminho_md_ultimo else {}

        return {
            "status": "ok",
            "resumo": {
                "processo": processo.dict(),
                "primeiro_documento": doc_primeiro,
                "resumo_primeiro": resposta_ia_primeiro,
                "ultimo_documento": doc_ultimo,
                "resumo_ultimo": resposta_ia_ultimo,
                "usuario_ultimo_andamento": usuario
            }
        }

    except Exception as e:
        logging.exception("Erro ao processar o resumo")
        raise HTTPException(status_code=500, detail=str(e))
