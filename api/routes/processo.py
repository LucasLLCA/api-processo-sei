import asyncio
import logging
from fastapi import APIRouter, HTTPException
from ..sei import listar_documentos, listar_tarefa, consultar_documento, baixar_documento
from ..openai_client import enviar_para_ia_conteudo, enviar_para_ia_conteudo_md, enviar_documento_ia_conteudo
from ..utils import ler_conteudo_md
from ..models import ErrorDetail, ErrorType, Retorno
from ..cache import cache, gerar_chave_processo, gerar_chave_documento, gerar_chave_andamento, gerar_chave_resumo

logger = logging.getLogger(__name__)

router = APIRouter()

# TTL padrão de 48 horas
CACHE_TTL = 172800


@router.get("/andamento/{numero_processo}", response_model=Retorno)
async def andamento(numero_processo: str, token: str, id_unidade: str):
    """
    Retorna o andamento atual do processo e um resumo do último documento.

    Args:
        numero_processo (str): Número do processo no SEI
        token (str): Token de autenticação do SEI
        id_unidade (str): ID da unidade no SEI

    Returns:
        Retorno: Objeto contendo o status, andamento e resumo do último documento
    """
    try:
        # Verifica cache primeiro
        cache_key = gerar_chave_andamento(numero_processo)
        cached_result = await cache.get(cache_key)
        if cached_result:
            logger.debug(f"Retornando andamento do cache para processo {numero_processo}")
            return Retorno(status="ok", andamento=cached_result.get("andamento"), resumo=cached_result.get("resumo"))

        # Busca documentos e andamentos em paralelo
        documentos, andamentos = await asyncio.gather(
            listar_documentos(token, numero_processo, id_unidade),
            listar_tarefa(token, numero_processo, id_unidade)
        )

        if not documentos:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Nenhum documento encontrado para este processo",
                    details={"numero_processo": numero_processo}
                ).dict()
            )

        ultimo = documentos[-1]

        # Consulta documento e baixa conteúdo em paralelo
        doc_ultimo, md_ultimo = await asyncio.gather(
            consultar_documento(token, id_unidade, ultimo["DocumentoFormatado"]),
            baixar_documento(token, id_unidade, ultimo["DocumentoFormatado"], numero_processo)
        )

        # Envia para IA
        resposta_ia_ultimo = await enviar_para_ia_conteudo(ler_conteudo_md(md_ultimo)) if md_ultimo else {}

        # Armazena no cache
        resultado = {
            "andamento": doc_ultimo,
            "resumo": resposta_ia_ultimo
        }
        await cache.set(cache_key, resultado, ttl=CACHE_TTL)

        return Retorno(
            status="ok",
            andamento=doc_ultimo,
            resumo=resposta_ia_ultimo
        )

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao processar andamento: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o andamento do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )


@router.get("/resumo/{numero_processo}", response_model=Retorno)
async def resumo(numero_processo: str, token: str, id_unidade: str):
    """
    Retorna um resumo do processo, incluindo o primeiro e último documento.

    Args:
        numero_processo (str): Número do processo no SEI
        token (str): Token de autenticação do SEI
        id_unidade (str): ID da unidade no SEI

    Returns:
        Retorno: Objeto contendo o status e resumo do processo
    """
    try:
        # Verifica cache primeiro
        cache_key = gerar_chave_resumo(numero_processo)
        cached_result = await cache.get(cache_key)
        if cached_result:
            logger.debug(f"Retornando resumo do cache para processo {numero_processo}")
            return Retorno(status="ok", resumo=cached_result)

        documentos = await listar_documentos(token, numero_processo, id_unidade)

        if not documentos:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Nenhum documento encontrado para este processo",
                    details={"numero_processo": numero_processo}
                ).dict()
            )

        primeiro = documentos[0]
        ultimo = documentos[-1]

        # Busca todos os dados em paralelo
        doc_primeiro, doc_ultimo, md_primeiro, md_ultimo = await asyncio.gather(
            consultar_documento(token, id_unidade, primeiro["DocumentoFormatado"]),
            consultar_documento(token, id_unidade, ultimo["DocumentoFormatado"]),
            baixar_documento(token, id_unidade, primeiro["DocumentoFormatado"], numero_processo),
            baixar_documento(token, id_unidade, ultimo["DocumentoFormatado"], numero_processo)
        )

        # Envia para IA em paralelo
        resposta_ia_primeiro, resposta_ia_ultimo = await asyncio.gather(
            enviar_para_ia_conteudo(ler_conteudo_md(md_primeiro)) if md_primeiro else asyncio.sleep(0, result={}),
            enviar_para_ia_conteudo(ler_conteudo_md(md_ultimo)) if md_ultimo else asyncio.sleep(0, result={})
        )

        resultado = {
            "processo": {
                "numero": numero_processo,
                "id_unidade": id_unidade
            },
            "primeiro_documento": doc_primeiro,
            "resumo_primeiro": resposta_ia_primeiro,
            "ultimo_documento": doc_ultimo,
            "resumo_ultimo": resposta_ia_ultimo
        }

        # Armazena no cache
        await cache.set(cache_key, resultado, ttl=CACHE_TTL)

        return Retorno(status="ok", resumo=resultado)

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao processar resumo: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o resumo do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )


@router.get("/resumo-completo/{numero_processo}", response_model=Retorno)
async def resumo_completo(numero_processo: str, token: str, id_unidade: str):
    """
    Retorna uma análise completa do processo, combinando o primeiro e último documento.

    Args:
        numero_processo (str): Número do processo no SEI
        token (str): Token de autenticação do SEI
        id_unidade (str): ID da unidade no SEI

    Returns:
        Retorno: Objeto contendo o status e resumo completo do processo
    """
    try:
        documentos = await listar_documentos(token, numero_processo, id_unidade)

        if not documentos:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Nenhum documento encontrado para este processo",
                    details={"numero_processo": numero_processo}
                ).dict()
            )

        primeiro = documentos[0]

        # Gera chave de cache com processo, primeiro e último documento
        id_primeiro_doc = primeiro.get("IdDocumento", primeiro.get("DocumentoFormatado"))
        cache_key = gerar_chave_processo(numero_processo, id_primeiro_doc)

        # Tenta obter do cache
        cached_result = await cache.get(cache_key)
        if cached_result:
            logger.debug(f"Retornando resultado do cache para processo {numero_processo}")
            return Retorno(status="ok", resumo=cached_result)

        logger.debug(f"Iniciando processamento do processo {numero_processo}")
        logger.debug(f"Primeiro documento: {primeiro['DocumentoFormatado']}")

        # Busca todos os dados em paralelo
        results = await asyncio.gather(
            consultar_documento(token, id_unidade, primeiro["DocumentoFormatado"]),
            baixar_documento(token, id_unidade, primeiro["DocumentoFormatado"], numero_processo),
            return_exceptions=True
        )

        doc_primeiro = results[0] if not isinstance(results[0], Exception) else {}
        md_primeiro = results[1] if not isinstance(results[1], Exception) else None

        if isinstance(results[0], Exception):
            logger.error(f"Falha ao consultar primeiro documento: {str(results[0])}")
        else:
            logger.debug(f"Primeiro documento consultado: {doc_primeiro.get('Titulo', 'Sem título')}")

        conteudo_combinado = ""
        if md_primeiro:
            try:
                conteudo_primeiro = ler_conteudo_md(md_primeiro)
                logger.debug(f"Conteúdo do primeiro documento: {len(conteudo_primeiro)} caracteres")
                conteudo_combinado += f"PRIMEIRO DOCUMENTO:\n{conteudo_primeiro}\n\n"
            except Exception as e:
                logger.error(f"Falha ao ler conteúdo do primeiro documento: {str(e)}")

        logger.debug(f"Tamanho total do conteúdo combinado: {len(conteudo_combinado)} caracteres")

        try:
            resposta_ia_combinada = await enviar_para_ia_conteudo_md(conteudo_combinado) if conteudo_combinado else {}
            logger.debug(f"Resposta da IA recebida: {resposta_ia_combinada.get('status', 'sem status')}")
        except Exception as e:
            logger.error(f"Falha ao obter resposta da IA: {str(e)}")
            resposta_ia_combinada = {"status": "erro", "resposta_ia": f"Erro ao processar: {str(e)}"}

        # Monta o resultado
        resultado = {
            "processo": {
                "numero": numero_processo,
                "id_unidade": id_unidade
            },
            "primeiro_documento": doc_primeiro,
            "resumo_combinado": resposta_ia_combinada
        }

        # Armazena no cache
        await cache.set(cache_key, resultado, ttl=CACHE_TTL)

        return Retorno(status="ok", resumo=resultado)

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao processar resumo completo: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o resumo completo do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )


@router.get("/resumo-documento/{documento_formatado}", response_model=Retorno)
async def resumo_documento(documento_formatado: str, token: str, id_unidade: str):
    """
    Retorna uma análise completa de um documento.

    Args:
        documento_formatado (str): Número do documento formatado
        token (str): Token de autenticação do SEI
        id_unidade (str): ID da unidade no SEI

    Returns:
        Retorno: Objeto contendo o status e resumo completo do documento
    """
    try:
        # Gera chave de cache com o ID do documento
        cache_key = gerar_chave_documento(documento_formatado)

        # Tenta obter do cache
        cached_result = await cache.get(cache_key)
        if cached_result:
            logger.debug(f"Retornando resultado do cache para documento {documento_formatado}")
            return Retorno(status="ok", resumo=cached_result)

        # Busca documento e conteúdo em paralelo
        doc, md = await asyncio.gather(
            consultar_documento(token, id_unidade, documento_formatado),
            baixar_documento(token, id_unidade, documento_formatado)
        )

        conteudo = ""
        if md:
            conteudo = ler_conteudo_md(md)
        else:
            logger.warning(f"Documento {documento_formatado} não retornou conteúdo MD, usando dados básicos")

        if not conteudo:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Documento não encontrado ou não foi possível processar o conteúdo",
                    details={"documento_formatado": documento_formatado}
                ).dict()
            )

        resposta_ia = await enviar_documento_ia_conteudo(conteudo)

        # Monta o resultado
        resultado = {
            "documento": doc,
            "resumo": resposta_ia
        }

        # Armazena no cache
        await cache.set(cache_key, resultado, ttl=CACHE_TTL)

        return Retorno(status="ok", resumo=resultado)

    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Erro ao processar resumo do documento: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o resumo do documento",
                details={"error": str(e), "documento_formatado": documento_formatado}
            ).dict()
        )
