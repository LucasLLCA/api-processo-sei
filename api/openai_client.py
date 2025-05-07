import os
from openai import OpenAI
from fastapi import HTTPException
from .config import OPENAI_CONFIG

client = OpenAI(
    api_key=OPENAI_CONFIG["api_key"],
    base_url=OPENAI_CONFIG["base_url"]
)

def enviar_para_ia(caminho_arquivo_md: str) -> dict:
    if not os.path.isfile(caminho_arquivo_md):
        raise HTTPException(status_code=400, detail="Arquivo não encontrado")

    with open(caminho_arquivo_md, "r", encoding="utf-8") as f:
        conteudo = f.read()

    try:
        resposta = client.chat.completions.create(
            model="Qwen3-30B-A3B",
            messages=[
                {"role": "system", "content": "Você é um assistente jurídico que lê documentos Markdown e resume seu conteúdo."},
                {"role": "user", "content": f"Leia o documento abaixo e diga do que se trata:\n\n{conteudo}"}
            ],
            temperature=0.7,
        )
        return {"status": "ok", "resposta_ia": resposta.choices[0].message.content.strip()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")
