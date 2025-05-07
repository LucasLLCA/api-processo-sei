import os
import re

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

def ler_arquivo_md(caminho_md: str) -> str:
    if not caminho_md or not os.path.isfile(caminho_md):
        return ""
    with open(caminho_md, "r", encoding="utf-8") as f:
        return f.read()

