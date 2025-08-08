import os
import re
import io
from minio import Minio
from minio.error import S3Error

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
    Lê arquivo MD diretamente do MinIO
    """
    from .config import settings
    
    if not object_name:
        return ""
    
    try:
        # Inicializar cliente MinIO
        minio_client = Minio(
            settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=True
        )
        
        # Baixar o objeto do MinIO
        response = minio_client.get_object(settings.MINIO_BUCKET, object_name)
        content = response.read().decode('utf-8')
        response.close()
        response.release_conn()
        
        return content
    except S3Error as e:
        print(f"[WARN] Falha ao ler arquivo MD do MinIO: {str(e)}")
        return ""
    except Exception as e:
        print(f"[WARN] MinIO inacessível (leitura MD): {str(e)}")
        return ""

