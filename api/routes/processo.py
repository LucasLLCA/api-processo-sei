from fastapi import APIRouter, HTTPException
from ..sei import listar_documentos, listar_tarefa, consultar_documento, baixar_documento
from ..openai_client import enviar_para_ia_conteudo, enviar_para_ia_conteudo_md
from ..utils import ler_arquivo_md
from ..models import ErrorDetail, ErrorType, Retorno
from concurrent.futures import ThreadPoolExecutor

router = APIRouter()

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
        documentos = listar_documentos(token, numero_processo, id_unidade)
        andamentos = listar_tarefa(token, numero_processo, id_unidade)

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
            fut_doc_ultimo = executor.submit(consultar_documento, token, id_unidade, ultimo["DocumentoFormatado"])
            fut_md_ultimo = executor.submit(baixar_documento, token, id_unidade, ultimo["DocumentoFormatado"])

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
        documentos = listar_documentos(token, numero_processo, id_unidade)

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
            fut_doc_primeiro = executor.submit(consultar_documento, token, id_unidade, primeiro["DocumentoFormatado"])
            fut_doc_ultimo = executor.submit(consultar_documento, token, id_unidade, ultimo["DocumentoFormatado"])
            fut_md_primeiro = executor.submit(baixar_documento, token, id_unidade, primeiro["DocumentoFormatado"])
            fut_md_ultimo = executor.submit(baixar_documento, token, id_unidade, ultimo["DocumentoFormatado"])

            doc_primeiro = fut_doc_primeiro.result()
            doc_ultimo = fut_doc_ultimo.result()
            md_primeiro = fut_md_primeiro.result()
            md_ultimo = fut_md_ultimo.result()

            resposta_ia_primeiro = enviar_para_ia_conteudo(ler_arquivo_md(md_primeiro)) if md_primeiro else {}
            resposta_ia_ultimo = enviar_para_ia_conteudo(ler_arquivo_md(md_ultimo)) if md_ultimo else {}

        return Retorno(
            status="ok",
            resumo={
                "processo": {
                    "numero": numero_processo,
                    "id_unidade": id_unidade
                },
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
        documentos = listar_documentos(token, numero_processo, id_unidade)

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
            print(f"[DEBUG] Iniciando processamento do processo {numero_processo}")
            print(f"[DEBUG] Primeiro documento: {primeiro['DocumentoFormatado']}")
            print(f"[DEBUG] Último documento: {ultimo['DocumentoFormatado']}")
            
            fut_doc_primeiro = executor.submit(consultar_documento, token, id_unidade, primeiro["DocumentoFormatado"])
            fut_doc_ultimo = executor.submit(consultar_documento, token, id_unidade, ultimo["DocumentoFormatado"])
            fut_md_primeiro = executor.submit(baixar_documento, token, id_unidade, primeiro["DocumentoFormatado"])
            fut_md_ultimo = executor.submit(baixar_documento, token, id_unidade, ultimo["DocumentoFormatado"])

            try:
                doc_primeiro = fut_doc_primeiro.result()
                print(f"[DEBUG] Primeiro documento consultado com sucesso: {doc_primeiro.get('Titulo', 'Sem título')}")
            except Exception as e:
                print(f"[ERRO] Falha ao consultar primeiro documento: {str(e)}")
                doc_primeiro = {}

            try:
                doc_ultimo = fut_doc_ultimo.result()
                print(f"[DEBUG] Último documento consultado com sucesso: {doc_ultimo.get('Titulo', 'Sem título')}")
            except Exception as e:
                print(f"[ERRO] Falha ao consultar último documento: {str(e)}")
                doc_ultimo = {}

            try:
                md_primeiro = fut_md_primeiro.result()
                print(f"[DEBUG] Primeiro documento baixado com sucesso: {md_primeiro if md_primeiro else 'Nenhum arquivo'}")
            except Exception as e:
                print(f"[ERRO] Falha ao baixar primeiro documento: {str(e)}")
                md_primeiro = None

            try:
                md_ultimo = fut_md_ultimo.result()
                print(f"[DEBUG] Último documento baixado com sucesso: {md_ultimo if md_ultimo else 'Nenhum arquivo'}")
            except Exception as e:
                print(f"[ERRO] Falha ao baixar último documento: {str(e)}")
                md_ultimo = None

            conteudo_combinado = ""
            if md_primeiro:
                try:
                    conteudo_primeiro = ler_arquivo_md(md_primeiro)
                    print(f"[DEBUG] Conteúdo do primeiro documento lido: {len(conteudo_primeiro)} caracteres")
                    conteudo_combinado += f"PRIMEIRO DOCUMENTO:\n{conteudo_primeiro}\n\n"
                except Exception as e:
                    print(f"[ERRO] Falha ao ler conteúdo do primeiro documento: {str(e)}")

            if md_ultimo:
                try:
                    conteudo_ultimo = ler_arquivo_md(md_ultimo)
                    print(f"[DEBUG] Conteúdo do último documento lido: {len(conteudo_ultimo)} caracteres")
                    conteudo_combinado += f"ÚLTIMO DOCUMENTO:\n{conteudo_ultimo}"
                except Exception as e:
                    print(f"[ERRO] Falha ao ler conteúdo do último documento: {str(e)}")

            print(f"[DEBUG] Tamanho total do conteúdo combinado: {len(conteudo_combinado)} caracteres")

            try:
                resposta_ia_combinada = enviar_para_ia_conteudo_md(conteudo_combinado) if conteudo_combinado else {}
                print(f"[DEBUG] Resposta da IA recebida: {resposta_ia_combinada.get('status', 'sem status')}")
            except Exception as e:
                print(f"[ERRO] Falha ao obter resposta da IA: {str(e)}")
                resposta_ia_combinada = {"status": "erro", "resposta_ia": f"Erro ao processar: {str(e)}"}

        return Retorno(
            status="ok",
            resumo={
                "processo": {
                    "numero": numero_processo,
                    "id_unidade": id_unidade
                },
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
        doc = consultar_documento(token, id_unidade, documento_formatado)
        md = baixar_documento(token, id_unidade, documento_formatado)

        if not md:
            raise HTTPException(
                status_code=404,
                detail=ErrorDetail(
                    type=ErrorType.NOT_FOUND,
                    message="Documento não encontrado ou não é um documento HTML",
                    details={"documento_formatado": documento_formatado}
                ).dict()
            )

        conteudo = ler_arquivo_md(md)
        resposta_ia = enviar_para_ia_conteudo(conteudo)

        return Retorno(
            status="ok",
            resumo={
                "documento": doc,
                "resumo": resposta_ia
            }
        )

    except HTTPException as he:
        raise he
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=ErrorDetail(
                type=ErrorType.PROCESSING_ERROR,
                message="Erro ao processar o resumo do documento",
                details={"error": str(e), "documento_formatado": documento_formatado}
            ).dict()
        )
    