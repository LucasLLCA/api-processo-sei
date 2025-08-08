import os
import re
import io
import hashlib
import hmac
import base64
import requests
from datetime import datetime

def _gerar_assinatura_aws(method: str, bucket: str, key: str, headers: dict, access_key: str, secret_key: str):
    """
    Gera assinatura AWS v2 para requisições S3/MinIO
    """
    # String to sign
    headers_to_sign = {}
    for header_name, header_value in headers.items():
        if header_name.lower().startswith('x-amz-'):
            headers_to_sign[header_name.lower()] = header_value
    
    canonical_headers = ''.join(f"{k}:{v}\n" for k, v in sorted(headers_to_sign.items()))
    
    string_to_sign = f"{method}\n\n{headers.get('content-type', '')}\n{headers.get('date', '')}\n{canonical_headers}/{bucket}/{key}"
    
    # Gerar assinatura
    signature = base64.b64encode(
        hmac.new(secret_key.encode(), string_to_sign.encode(), hashlib.sha1).digest()
    ).decode()
    
    return f"AWS {access_key}:{signature}"

def _ler_objeto_minio(bucket: str, key: str, access_key: str, secret_key: str, endpoint: str):
    """
    Lê um objeto do MinIO usando GET
    """
    try:
        url = f"http://{endpoint}/{bucket}/{key}"
        date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers = {'date': date}
        
        auth = _gerar_assinatura_aws('GET', bucket, key, headers, access_key, secret_key)
        headers['authorization'] = auth
        
        response = requests.get(url, headers=headers, timeout=30, verify=False)
        if response.status_code == 200:
            return response.content.decode('utf-8')
        else:
            print(f"[WARN] Erro ao ler objeto {key}: HTTP {response.status_code}")
            return ""
    except Exception as e:
        print(f"[WARN] Erro ao ler objeto {key}: {str(e)}")
        return ""

def converte_html_para_markdown_memoria(html_content: str) -> str:
    """
    Converte conteúdo HTML para Markdown diretamente em memória
    """
    texto_limpo = re.sub(r'<[^>]+>', '', html_content)
    return texto_limpo

def ler_conteudo_md(md_data: str) -> str:
    """
    Lê conteúdo MD - pode ser um caminho do MinIO ou conteúdo direto
    """
    if not md_data:
        return ""
    
    # Se contém caracteres que indicam ser um caminho (/ ou extensão)
    if '/' in md_data or md_data.endswith('.md'):
        return ler_arquivo_md_minio(md_data)
    
    # Se parece ser conteúdo MD direto, retorna diretamente
    if len(md_data) > 100 or '\n' in md_data:
        print(f"[DEBUG] Usando conteúdo MD direto: {len(md_data)} caracteres")
        return md_data
    
    # Como fallback, tenta ler do MinIO
    return ler_arquivo_md_minio(md_data)

def ler_arquivo_md_minio(object_name: str) -> str:
    """
    Lê arquivo MD diretamente do MinIO usando requests
    """
    from .config import settings
    
    if not object_name:
        return ""
    
    try:
        content = _ler_objeto_minio(
            settings.MINIO_BUCKET, 
            object_name, 
            settings.MINIO_ACCESS_KEY, 
            settings.MINIO_SECRET_KEY, 
            settings.MINIO_ENDPOINT
        )
        return content
    except Exception as e:
        print(f"[WARN] Erro ao ler arquivo MD do MinIO: {str(e)}")
        return ""

