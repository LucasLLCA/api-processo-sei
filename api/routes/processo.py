from fastapi import APIRouter, HTTPException
from ..sei import obter_token, buscar_processo, listar_documentos, listar_tarefa, consultar_documento, baixar_documento
from ..openai_client import enviar_para_ia_conteudo, enviar_para_ia_conteudo_md
from ..utils import ler_arquivo_md
from ..models import ErrorDetail, ErrorType, Retorno
from concurrent.futures import ThreadPoolExecutor

router = APIRouter()

@router.get("/andamento/{numero_processo}", response_model=Retorno)
async def andamento(numero_processo: str):
    """
    Retorna o andamento atual do processo e um resumo do último documento.
    
    Args:
        numero_processo (str): Número do processo no SEI
        
    Returns:
        Retorno: Objeto contendo o status, andamento e resumo do último documento
    """
    try:
        token = obter_token()
        processo = buscar_processo(numero_processo)
        documentos = listar_documentos(token, processo.protocolo, processo.id_unidade)
        andamentos = listar_tarefa(token, processo.protocolo, processo.id_unidade)

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

        with ThreadPoolExecutor() as executor:
            fut_doc_ultimo = executor.submit(consultar_documento, token, processo.id_unidade, ultimo["DocumentoFormatado"])
            fut_md_ultimo = executor.submit(baixar_documento, token, processo.id_unidade, ultimo["DocumentoFormatado"])

            doc_ultimo = fut_doc_ultimo.result()
            md_ultimo = fut_md_ultimo.result()

            resposta_ia_ultimo = enviar_para_ia_conteudo(ler_arquivo_md(md_ultimo)) if md_ultimo else {}

        return Retorno(
            status="ok",
            andamento=doc_ultimo,
            resumo=resposta_ia_ultimo
        )

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o andamento do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )

@router.get("/resumo/{numero_processo}", response_model=Retorno)
async def resumo(numero_processo: str):
    """
    Retorna um resumo do processo, incluindo o primeiro e último documento.
    
    Args:
        numero_processo (str): Número do processo no SEI
        
    Returns:
        Retorno: Objeto contendo o status e resumo do processo
    """
    try:
        token = obter_token()
        processo = buscar_processo(numero_processo)
        documentos = listar_documentos(token, processo.protocolo, processo.id_unidade)

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

        with ThreadPoolExecutor() as executor:
            fut_doc_primeiro = executor.submit(consultar_documento, token, processo.id_unidade, primeiro["DocumentoFormatado"])
            fut_doc_ultimo = executor.submit(consultar_documento, token, processo.id_unidade, ultimo["DocumentoFormatado"])
            fut_md_primeiro = executor.submit(baixar_documento, token, processo.id_unidade, primeiro["DocumentoFormatado"])
            fut_md_ultimo = executor.submit(baixar_documento, token, processo.id_unidade, ultimo["DocumentoFormatado"])

            doc_primeiro = fut_doc_primeiro.result()
            doc_ultimo = fut_doc_ultimo.result()
            md_primeiro = fut_md_primeiro.result()
            md_ultimo = fut_md_ultimo.result()

            resposta_ia_primeiro = enviar_para_ia_conteudo(ler_arquivo_md(md_primeiro)) if md_primeiro else {}
            resposta_ia_ultimo = enviar_para_ia_conteudo(ler_arquivo_md(md_ultimo)) if md_ultimo else {}

        return Retorno(
            status="ok",
            resumo={
                "processo": processo.dict(),
                "primeiro_documento": doc_primeiro,
                "resumo_primeiro": resposta_ia_primeiro,
                "ultimo_documento": doc_ultimo,
                "resumo_ultimo": resposta_ia_ultimo
            }
        )

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o resumo do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        )

@router.get("/resumo-completo/{numero_processo}", response_model=Retorno)
async def resumo_completo(numero_processo: str):
    """
    Retorna uma análise completa do processo, combinando o primeiro e último documento.
    
    Args:
        numero_processo (str): Número do processo no SEI
        
    Returns:
        Retorno: Objeto contendo o status e resumo completo do processo
    """
    try:
        token = obter_token()
        processo = buscar_processo(numero_processo)
        documentos = listar_documentos(token, processo.protocolo, processo.id_unidade)

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

        with ThreadPoolExecutor() as executor:
            fut_doc_primeiro = executor.submit(consultar_documento, token, processo.id_unidade, primeiro["DocumentoFormatado"])
            fut_doc_ultimo = executor.submit(consultar_documento, token, processo.id_unidade, ultimo["DocumentoFormatado"])
            fut_md_primeiro = executor.submit(baixar_documento, token, processo.id_unidade, primeiro["DocumentoFormatado"])
            fut_md_ultimo = executor.submit(baixar_documento, token, processo.id_unidade, ultimo["DocumentoFormatado"])

            doc_primeiro = fut_doc_primeiro.result()
            doc_ultimo = fut_doc_ultimo.result()
            md_primeiro = fut_md_primeiro.result()
            md_ultimo = fut_md_ultimo.result()

            conteudo_combinado = ""
            if md_primeiro:
                conteudo_combinado += f"PRIMEIRO DOCUMENTO:\n{ler_arquivo_md(md_primeiro)}\n\n"
            if md_ultimo:
                conteudo_combinado += f"ÚLTIMO DOCUMENTO:\n{ler_arquivo_md(md_ultimo)}"

            resposta_ia_combinada = enviar_para_ia_conteudo_md(conteudo_combinado) if conteudo_combinado else {}

        return Retorno(
            status="ok",
            resumo={
                "processo": processo.dict(),
                "primeiro_documento": doc_primeiro,
                "ultimo_documento": doc_ultimo,
                "resumo_combinado": resposta_ia_combinada
            }
        )

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o resumo completo do processo",
                details={"error": str(e), "numero_processo": numero_processo}
            ).dict()
        ) 