import os
import re
import io
from minio import Minio
from minio.error import S3Error

def _get_minio_client():
    """
    Cria e retorna um cliente MinIO
    """
    from .config import settings
    import urllib3
    
    # Desabilitar warnings de SSL não verificado
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    return Minio(
        settings.MINIO_ENDPOINT,
        access_key=settings.MINIO_ACCESS_KEY,
        secret_key=settings.MINIO_SECRET_KEY,
        secure=True,
        http_client=urllib3.PoolManager(
            cert_reqs='CERT_NONE',
            ca_cert_dir=None,
            ca_certs=None
        )
    )

def _ler_objeto_minio(bucket: str, key: str):
    """
    Lê um objeto do MinIO usando o client
    """
    try:
        client = _get_minio_client()
        response = client.get_object(bucket, key)
        content = response.read().decode('utf-8')
        response.close()
        response.release_conn()
        return content
    except S3Error as e:
        if e.code == 'NoSuchKey':
            print(f"[WARN] Objeto não encontrado: {key}")
        else:
            print(f"[WARN] Erro S3 ao ler objeto {key}: {str(e)}")
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
    Lê arquivo MD diretamente do MinIO usando client
    """
    from .config import settings
    
    if not object_name:
        return ""
    
    try:
        content = _ler_objeto_minio(settings.MINIO_BUCKET, object_name)
        return content
    except Exception as e:
        print(f"[WARN] Erro ao ler arquivo MD do MinIO: {str(e)}")
        return ""

