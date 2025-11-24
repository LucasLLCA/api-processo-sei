import logging
from openai import AsyncOpenAI
from fastapi import HTTPException
from .config import settings

logger = logging.getLogger(__name__)

logger.info(f"OpenAI configurado - URL: {settings.OPENAI_BASE_URL}, API Key definida: {bool(settings.OPENAI_API_KEY)}")

client = AsyncOpenAI(
    base_url=settings.OPENAI_BASE_URL,
    api_key=settings.OPENAI_API_KEY
)


async def enviar_para_ia_conteudo(conteudo_md: str) -> dict:
    if not conteudo_md.strip():
        return {"status": "erro", "resposta_ia": "Conteúdo Markdown vazio"}

    try:
        logger.debug(f"Enviando conteúdo para IA. Tamanho: {len(conteudo_md)} caracteres")
        resposta = await client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Você é um assistente jurídico especializado..."},
                {"role": "user", "content": f"Leia cuidadosamente o documento Markdown abaixo e produza um relatório detalhado...\n\nDocumento:\n\n{conteudo_md}"}
            ],
            temperature=0.7,
        )
        logger.debug("Resposta da IA recebida com sucesso")
        return {"status": "ok", "resposta_ia": resposta.choices[0].message.content.strip()}

    except Exception as e:
        logger.error(f"Falha ao consultar IA: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")
    

async def enviar_para_ia_conteudo_md(conteudo_md: str) -> dict:
    if not conteudo_md.strip():
        return {"status": "erro", "resposta_ia": "Conteúdo Markdown vazio"}

    try:
        logger.debug(f"Enviando conteúdo para IA (MD). Tamanho: {len(conteudo_md)} caracteres")
        resposta = await client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Você é um assistente jurídico especializado em analisar processos administrativos. Sua tarefa é produzir um resumo claro e conciso em dois parágrafos, integrando as informações dos documentos de forma coerente."},
                {"role": "user", "content": f"""Analise os documentos abaixo e produza um resumo que integre as informações de forma coerente:
                Documentos:
                {conteudo_md}"""}
            ],
            temperature=0.7,
        )
        logger.debug("Resposta da IA (MD) recebida com sucesso")
        return {"status": "ok", "resposta_ia": resposta.choices[0].message.content.strip()}

    except Exception as e:
        logger.error(f"Falha ao consultar IA (MD): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")

async def enviar_documento_ia_conteudo(conteudo_md: str) -> dict:
    if not conteudo_md.strip():
        return {"status": "erro", "resposta_ia": "Conteúdo Markdown vazio"}

    try:
        logger.debug(f"Enviando documento para IA. Tamanho: {len(conteudo_md)} caracteres")
        resposta = await client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Você é um assistente jurídico especializado..."},
                {"role": "user", "content": f"Leia cuidadosamente o documento Markdown abaixo e produza um resumo de maximo 300 caracteres...\n\nDocumento:\n\n{conteudo_md}"}
            ],
            temperature=0.7,
        )
        logger.debug("Resposta da IA recebida com sucesso")
        return {"status": "ok", "resposta_ia": resposta.choices[0].message.content.strip()}

    except Exception as e:
        logger.error(f"Falha ao consultar IA: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")