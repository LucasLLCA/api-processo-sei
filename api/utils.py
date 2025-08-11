import re

def converte_html_para_markdown_memoria(html_content: str) -> str:
    """
    Converte conteúdo HTML para Markdown diretamente em memória
    """
    texto_limpo = re.sub(r'<[^>]+>', '', html_content)
    return texto_limpo

def ler_conteudo_md(md_data: str) -> str:
    """
    Processa conteúdo MD que agora vem sempre como string da memória
    """
    if not md_data:
        return ""
    
    print(f"[DEBUG] Processando conteúdo MD em memória: {len(md_data)} caracteres")
    return md_data

