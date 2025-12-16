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

        if tipo_arquivo == "html":
            resposta = await client.chat.completions.create(
                model=modelo,
                messages=[
                    {"role": "system", "content": "Você é um assistente jurídico especializado em analisar processos administrativos. Sua tarefa é produzir um resumo claro e conciso em dois parágrafos, integrando as informações dos documentos de forma coerente."},
                    {"role": "user", "content": f"""Analise os documentos abaixo e produza um resumo que integre as informações de forma coerente:
                    Documentos:
                    {conteudo_md}"""}
                ],
                temperature=0.7,
            )
        else:  # PDF
            # Para PDF, converter para imagens e enviar
            import base64
            from pdf2image import convert_from_bytes
            from io import BytesIO

            try:
                # Converter PDF em imagens (primeira página ou todas)
                images = convert_from_bytes(conteudo_md, first_page=1, last_page=5)  # Limitar a 5 páginas
                logger.debug(f"PDF convertido em {len(images)} imagem(ns)")

                # Preparar mensagens com as imagens
                image_contents = []
                for image in images:
                    # Converter imagem PIL para base64
                    buffered = BytesIO()
                    image.save(buffered, format="PNG")
                    img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')

                    image_contents.append({
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{img_base64}"
                        }
                    })

                # Criar mensagem com todas as imagens
                user_content = [
                    {
                        "type": "text",
                        "text": "Analise as páginas do documento PDF abaixo e produza um resumo que integre as informações de forma coerente:"
                    }
                ] + image_contents

                resposta = await client.chat.completions.create(
                    model=modelo,
                    messages=[
                        {"role": "system", "content": "Você é um assistente jurídico especializado em analisar processos administrativos. Sua tarefa é produzir um resumo claro e conciso em dois parágrafos, integrando as informações dos documentos de forma coerente."},
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

    except Exception as e:
        logger.error(f"Falha ao consultar IA (tipo: {tipo_arquivo}): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")

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
            # Para PDF, converter para imagens e enviar
            import base64
            from pdf2image import convert_from_bytes
            from io import BytesIO

            try:
                # Converter PDF em imagens (limitar a 5 páginas)
                images = convert_from_bytes(conteudo_md, first_page=1, last_page=5)
                logger.debug(f"PDF convertido em {len(images)} imagem(ns)")

                # Preparar mensagens com as imagens
                image_contents = []
                for image in images:
                    # Converter imagem PIL para base64
                    buffered = BytesIO()
                    image.save(buffered, format="PNG")
                    img_base64 = base64.b64encode(buffered.getvalue()).decode('utf-8')

                    image_contents.append({
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{img_base64}"
                        }
                    })

                # Criar mensagem com todas as imagens
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

    except Exception as e:
        logger.error(f"Falha ao consultar IA (tipo: {tipo_arquivo}): {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erro ao consultar IA: {str(e)}")