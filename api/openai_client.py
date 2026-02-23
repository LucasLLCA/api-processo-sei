import asyncio
import base64
import logging
from io import BytesIO

import httpx
from openai import AsyncOpenAI
from fastapi import HTTPException
from .config import settings

logger = logging.getLogger(__name__)

logger.info(f"OpenAI configurado - URL: {settings.OPENAI_BASE_URL}, API Key definida: {bool(settings.OPENAI_API_KEY)}")

client = AsyncOpenAI(
    base_url=settings.OPENAI_BASE_URL,
    api_key=settings.OPENAI_API_KEY,
    timeout=httpx.Timeout(float(settings.OPENAI_TIMEOUT), connect=10.0),
)


def _pdf_para_imagens_base64_sync(pdf_bytes: bytes, max_pages: int = 5) -> list[dict]:
    """
    Converte PDF em lista de objetos image_url para a API de visão.
    Execução síncrona (CPU-bound) - deve ser chamada via asyncio.to_thread().
    """
    from pdf2image import convert_from_bytes

    images = convert_from_bytes(pdf_bytes, first_page=1, last_page=max_pages)
    logger.debug(f"PDF convertido em {len(images)} imagem(ns)")

    image_contents = []
    for image in images:
        buffered = BytesIO()
        image.save(buffered, format="PNG")
        img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')

        image_contents.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/png;base64,{img_base64}"
            }
        })

    return image_contents


async def _pdf_para_imagens_base64(pdf_bytes: bytes, max_pages: int = 5) -> list[dict]:
    """
    Wrapper async que executa a conversão PDF→imagens em thread separada
    para não bloquear o event loop.
    """
    return await asyncio.to_thread(_pdf_para_imagens_base64_sync, pdf_bytes, max_pages)


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

    except httpx.TimeoutException as e:
        logger.error(f"Timeout ao consultar IA após {settings.OPENAI_TIMEOUT}s: {str(e)}")
        raise HTTPException(status_code=504, detail=f"Timeout ao consultar IA: a requisição excedeu {settings.OPENAI_TIMEOUT}s")
    except Exception as e:
        logger.error(f"Falha ao consultar IA: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")


async def enviar_para_ia_conteudo_md(conteudo_md: str, tipo_arquivo: str = "html") -> dict:
    """
    Envia conteúdo para IA usando o modelo apropriado baseado no tipo de arquivo

    Args:
        conteudo_md: Conteúdo em markdown (para HTML) ou bytes (para PDF)
        tipo_arquivo: Tipo do arquivo ('html' ou 'pdf')
    """
    if tipo_arquivo == "html":
        if not conteudo_md.strip():
            return {"status": "erro", "resposta_ia": "Conteúdo Markdown vazio"}

        modelo = settings.OPENAI_MODEL_TEXTO
        logger.debug(f"Usando modelo TEXTO: {modelo}")
    elif tipo_arquivo == "pdf":
        modelo = settings.OPENAI_MODEL_VISAO
        logger.debug(f"Usando modelo VISÃO: {modelo}")
    else:
        return {"status": "erro", "resposta_ia": f"Tipo de arquivo não suportado: {tipo_arquivo}"}

    try:
        logger.debug(f"Enviando conteúdo para IA (tipo: {tipo_arquivo}). Modelo: {modelo}")

        system_resumo = (
            "Você é um assistente jurídico especializado em analisar processos administrativos. "
            "Produza um resumo estruturado no seguinte formato:\n"
            "1. Comece com UMA ÚNICA frase-síntese que resuma o processo como um todo.\n"
            "2. Em seguida, liste os pontos principais em tópicos usando '•', um por linha.\n"
            "Seja claro, objetivo e conciso. Não repita informações entre a frase-síntese e os tópicos."
        )

        if tipo_arquivo == "html":
            resposta = await client.chat.completions.create(
                model=modelo,
                messages=[
                    {"role": "system", "content": system_resumo},
                    {"role": "user", "content": f"Analise os documentos abaixo e produza o resumo estruturado:\n\nDocumentos:\n{conteudo_md}"}
                ],
                temperature=0.7,
                max_tokens=500,
            )
        else:  # PDF
            try:
                image_contents = await _pdf_para_imagens_base64(conteudo_md)

                user_content = [
                    {
                        "type": "text",
                        "text": "Analise as páginas do documento PDF abaixo e produza o resumo estruturado:"
                    }
                ] + image_contents

                resposta = await client.chat.completions.create(
                    model=modelo,
                    messages=[
                        {"role": "system", "content": system_resumo},
                        {
                            "role": "user",
                            "content": user_content
                        }
                    ],
                    temperature=0.7,
                    max_tokens=500,
                )
            except ImportError:
                logger.error("pdf2image não está instalado. Instale com: pip install pdf2image")
                return {"status": "erro", "resposta_ia": "Erro: biblioteca pdf2image não disponível para processar PDF"}
            except Exception as pdf_error:
                logger.error(f"Erro ao processar PDF: {str(pdf_error)}")
                return {"status": "erro", "resposta_ia": f"Erro ao processar PDF: {str(pdf_error)}"}

        logger.debug(f"Resposta da IA (tipo: {tipo_arquivo}) recebida com sucesso")
        return {"status": "ok", "resposta_ia": resposta.choices[0].message.content.strip()}

    except httpx.TimeoutException as e:
        logger.error(f"Timeout ao consultar IA (tipo: {tipo_arquivo}) após {settings.OPENAI_TIMEOUT}s: {str(e)}")
        raise HTTPException(status_code=504, detail=f"Timeout ao consultar IA: a requisição excedeu {settings.OPENAI_TIMEOUT}s")
    except Exception as e:
        logger.error(f"Falha ao consultar IA (tipo: {tipo_arquivo}): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")


async def enviar_para_ia_conteudo_md_stream(conteudo_md, tipo_arquivo: str = "html"):
    """
    Versão streaming de enviar_para_ia_conteudo_md.
    Yields text chunks conforme o LLM gera a resposta.
    """
    if tipo_arquivo == "html":
        if not conteudo_md.strip():
            return
        modelo = settings.OPENAI_MODEL_TEXTO
    elif tipo_arquivo == "pdf":
        modelo = settings.OPENAI_MODEL_VISAO
    else:
        return

    system_resumo = (
        "Você é um assistente jurídico especializado em analisar processos administrativos. "
        "Produza um resumo estruturado no seguinte formato:\n"
        "1. Comece com UMA ÚNICA frase-síntese que resuma o processo como um todo.\n"
        "2. Em seguida, liste os pontos principais em tópicos usando '•', um por linha.\n"
        "Seja claro, objetivo e conciso. Não repita informações entre a frase-síntese e os tópicos."
    )

    if tipo_arquivo == "html":
        messages = [
            {"role": "system", "content": system_resumo},
            {"role": "user", "content": f"Analise os documentos abaixo e produza o resumo estruturado:\n\nDocumentos:\n{conteudo_md}"}
        ]
    else:  # PDF
        image_contents = await _pdf_para_imagens_base64(conteudo_md)
        user_content = [
            {
                "type": "text",
                "text": "Analise as páginas do documento PDF abaixo e produza o resumo estruturado:"
            }
        ] + image_contents
        messages = [
            {"role": "system", "content": system_resumo},
            {"role": "user", "content": user_content}
        ]

    stream = await client.chat.completions.create(
        model=modelo,
        messages=messages,
        temperature=0.7,
        max_tokens=500,
        stream=True,
    )
    async for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content


async def enviar_situacao_atual_stream(entendimento: str, ultimo_doc_conteudo: str, ultimos_andamentos_texto: str):
    """
    Gera streaming da situação atual do processo com base no entendimento,
    último documento e últimos andamentos.
    """
    system_situacao = (
        "Você é um assistente jurídico. Com base no resumo do processo, no conteúdo do último documento "
        "adicionado e nas últimas atividades, produza uma análise estruturada da situação atual.\n"
        "Formato:\n"
        "1. Comece com UMA ÚNICA frase-síntese sobre o estado atual do processo.\n"
        "2. Em seguida, liste os pontos relevantes em tópicos usando '•', um por linha.\n"
        "Seja claro, objetivo e conciso."
    )

    messages = [
        {
            "role": "system",
            "content": system_situacao,
        },
        {
            "role": "user",
            "content": f"Resumo do processo:\n{entendimento}\n\nÚltimo documento adicionado:\n{ultimo_doc_conteudo}\n\nÚltimas atividades:\n{ultimos_andamentos_texto}",
        },
    ]

    stream = await client.chat.completions.create(
        model=settings.OPENAI_MODEL_TEXTO,
        messages=messages,
        temperature=0.7,
        max_tokens=500,
        stream=True,
    )
    async for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content


async def enviar_documento_ia_conteudo_stream(conteudo_md, tipo_arquivo: str = "html"):
    """
    Versão streaming de enviar_documento_ia_conteudo.
    Yields text chunks conforme o LLM gera a resposta.
    """
    if tipo_arquivo == "html":
        if not conteudo_md.strip():
            return
        modelo = settings.OPENAI_MODEL_TEXTO
    elif tipo_arquivo == "pdf":
        modelo = settings.OPENAI_MODEL_VISAO
    else:
        return

    if tipo_arquivo == "html":
        messages = [
            {"role": "system", "content": "Você é um assistente jurídico especializado..."},
            {"role": "user", "content": f"Leia cuidadosamente o documento Markdown abaixo e produza um resumo de maximo 300 caracteres...\n\nDocumento:\n\n{conteudo_md}"}
        ]
    else:  # PDF
        image_contents = await _pdf_para_imagens_base64(conteudo_md)
        user_content = [
            {
                "type": "text",
                "text": "Leia cuidadosamente as páginas do documento PDF abaixo e produza um resumo de máximo 300 caracteres:"
            }
        ] + image_contents
        messages = [
            {"role": "system", "content": "Você é um assistente jurídico especializado..."},
            {"role": "user", "content": user_content}
        ]

    stream = await client.chat.completions.create(
        model=modelo,
        messages=messages,
        temperature=0.7,
        stream=True,
    )
    async for chunk in stream:
        if chunk.choices and chunk.choices[0].delta.content:
            yield chunk.choices[0].delta.content


async def enviar_documento_ia_conteudo(conteudo_md: str, tipo_arquivo: str = "html") -> dict:
    """
    Envia documento para IA usando o modelo apropriado baseado no tipo de arquivo

    Args:
        conteudo_md: Conteúdo em markdown (para HTML) ou bytes (para PDF)
        tipo_arquivo: Tipo do arquivo ('html' ou 'pdf')
    """
    if tipo_arquivo == "html":
        if not conteudo_md.strip():
            return {"status": "erro", "resposta_ia": "Conteúdo Markdown vazio"}

        modelo = settings.OPENAI_MODEL_TEXTO
        logger.debug(f"Usando modelo TEXTO: {modelo}")
    elif tipo_arquivo == "pdf":
        modelo = settings.OPENAI_MODEL_VISAO
        logger.debug(f"Usando modelo VISÃO: {modelo}")
    else:
        return {"status": "erro", "resposta_ia": f"Tipo de arquivo não suportado: {tipo_arquivo}"}

    try:
        logger.debug(f"Enviando documento para IA (tipo: {tipo_arquivo}). Modelo: {modelo}")

        if tipo_arquivo == "html":
            resposta = await client.chat.completions.create(
                model=modelo,
                messages=[
                    {"role": "system", "content": "Você é um assistente jurídico especializado..."},
                    {"role": "user", "content": f"Leia cuidadosamente o documento Markdown abaixo e produza um resumo de maximo 300 caracteres...\n\nDocumento:\n\n{conteudo_md}"}
                ],
                temperature=0.7,
            )
        else:  # PDF
            try:
                image_contents = await _pdf_para_imagens_base64(conteudo_md)

                user_content = [
                    {
                        "type": "text",
                        "text": "Leia cuidadosamente as páginas do documento PDF abaixo e produza um resumo de máximo 300 caracteres:"
                    }
                ] + image_contents

                resposta = await client.chat.completions.create(
                    model=modelo,
                    messages=[
                        {"role": "system", "content": "Você é um assistente jurídico especializado..."},
                        {
                            "role": "user",
                            "content": user_content
                        }
                    ],
                    temperature=0.7,
                )
            except ImportError:
                logger.error("pdf2image não está instalado. Instale com: pip install pdf2image")
                return {"status": "erro", "resposta_ia": "Erro: biblioteca pdf2image não disponível para processar PDF"}
            except Exception as pdf_error:
                logger.error(f"Erro ao processar PDF: {str(pdf_error)}")
                return {"status": "erro", "resposta_ia": f"Erro ao processar PDF: {str(pdf_error)}"}

        logger.debug(f"Resposta da IA (tipo: {tipo_arquivo}) recebida com sucesso")
        return {"status": "ok", "resposta_ia": resposta.choices[0].message.content.strip()}

    except httpx.TimeoutException as e:
        logger.error(f"Timeout ao consultar IA (tipo: {tipo_arquivo}) após {settings.OPENAI_TIMEOUT}s: {str(e)}")
        raise HTTPException(status_code=504, detail=f"Timeout ao consultar IA: a requisição excedeu {settings.OPENAI_TIMEOUT}s")
    except Exception as e:
        logger.error(f"Falha ao consultar IA (tipo: {tipo_arquivo}): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")
