import os
import re
import io
from minio import Minio
from minio.error import S3Error

def converte_documentos_para_markdown(caminho_arquivo: str):
    if not os.path.isfile(caminho_arquivo):
        return
    with open(caminho_arquivo, "r", encoding="utf-8") as f:
        conteudo = f.read()

    texto_limpo = re.sub(r'<[^>]+>', '', conteudo)
    base, _ = os.path.splitext(caminho_arquivo)
    caminho_md = base + ".md"

    with open(caminho_md, "w", encoding="utf-8") as f:
        f.write(texto_limpo)

    return caminho_md

def converte_html_para_markdown_memoria(html_content: str) -> str:
    """
    Converte conteúdo HTML para Markdown diretamente em memória
    """
    texto_limpo = re.sub(r'<[^>]+>', '', html_content)
    return texto_limpo

def ler_arquivo_md(caminho_md: str) -> str:
    if not caminho_md or not os.path.isfile(caminho_md):
        return ""
    with open(caminho_md, "r", encoding="utf-8") as f:
        return f.read()

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
        print(f"[ERRO] Falha ao ler arquivo MD do MinIO: {str(e)}")
        return ""
    except Exception as e:
        print(f"[ERRO] Erro inesperado ao ler arquivo MD do MinIO: {str(e)}")
        return ""

