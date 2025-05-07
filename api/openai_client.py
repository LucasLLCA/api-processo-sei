import os
from openai import OpenAI
from fastapi import HTTPException
from .config import OPENAI_CONFIG

client = OpenAI(
    api_key=OPENAI_CONFIG["api_key"],
    base_url=OPENAI_CONFIG["base_url"]
)

def enviar_para_ia_conteudo(conteudo_md: str) -> dict:
    if not conteudo_md.strip():
        return {"status": "erro", "resposta_ia": "Conteúdo Markdown vazio"}
    
    try:
        resposta = client.chat.completions.create(
            model="Qwen3-30B-A3B",
            messages=[
                {"role": "system", "content": "Você é um assistente jurídico especializado..."},
                {"role": "user", "content": f"Leia cuidadosamente o documento Markdown abaixo e produza um relatório detalhado...\n\nDocumento:\n\n{conteudo_md}"}
            ],
            temperature=0.7,
        )
        return {"status": "ok", "resposta_ia": resposta.choices[0].message.content.strip()}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")