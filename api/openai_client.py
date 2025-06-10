import os
from openai import OpenAI
from fastapi import HTTPException
from .config import settings

print(f"[DEBUG] OPENAI_BASE_URL: {settings.OPENAI_BASE_URL}")
print(f"[DEBUG] OPENAI_API_KEY está configurada: {settings.OPENAI_API_KEY}")

client = OpenAI(
    base_url=settings.OPENAI_BASE_URL,
    api_key=settings.OPENAI_API_KEY
)


def enviar_para_ia_conteudo(conteudo_md: str) -> dict:
    if not conteudo_md.strip():
        return {"status": "erro", "resposta_ia": "Conteúdo Markdown vazio"}
    
    try:
        print(f"[DEBUG] Tentando enviar conteúdo para IA. Tamanho do conteúdo: {len(conteudo_md)} caracteres")
        resposta = client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Você é um assistente jurídico especializado..."},
                {"role": "user", "content": f"Leia cuidadosamente o documento Markdown abaixo e produza um relatório detalhado...\n\nDocumento:\n\n{conteudo_md}"}
            ],
            temperature=0.7,
        )
        print("[DEBUG] Resposta da IA recebida com sucesso")
        return {"status": "ok", "resposta_ia": resposta.choices[0].message.content.strip()}

    except Exception as e:
        print(f"[ERRO] Falha ao consultar IA: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")
    

def enviar_para_ia_conteudo_md(conteudo_md: str) -> dict:
    if not conteudo_md.strip():
        return {"status": "erro", "resposta_ia": "Conteúdo Markdown vazio"}
    
    try:
        print(f"[DEBUG] Tentando enviar conteúdo para IA (MD). Tamanho do conteúdo: {len(conteudo_md)} caracteres")
        resposta = client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Você é um assistente jurídico especializado em analisar processos administrativos. Sua tarefa é produzir um resumo claro e conciso em dois parágrafos, integrando as informações dos documentos de forma coerente."},
                {"role": "user", "content": f"""Analise os documentos abaixo e produza um resumo em dois parágrafos que integre as informações de forma coerente:
                1. No primeiro parágrafo, explique do que se trata o processo e seu objetivo.
                2. No segundo parágrafo, descreva a situação atual do processo.

                Produza um texto único e coerente, não separando por documentos. O texto deve fluir naturalmente entre os parágrafos.

                Documentos:
                {conteudo_md}"""}
            ],
            temperature=0.7,
        )
        print("[DEBUG] Resposta da IA (MD) recebida com sucesso")
        return {"status": "ok", "resposta_ia": resposta.choices[0].message.content.strip()}

    except Exception as e:
        print(f"[ERRO] Falha ao consultar IA (MD): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")
