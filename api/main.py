from fastapi import FastAPI, HTTPException
from .sei import obter_token, buscar_processo, listar_documentos, listar_tarefa, consultar_documento, baixar_documento
from concurrent.futures import ThreadPoolExecutor
from .openai_client import enviar_para_ia_conteudo
from .utils import ler_arquivo_md

app = FastAPI()


@app.get("/andamento/{numero_processo}")
def andamento(numero_processo: str):
    try:
        token = obter_token()
        processo = buscar_processo(numero_processo)
        documentos = listar_documentos(token, processo.protocolo, processo.id_unidade)
        andamentos = listar_tarefa(token, processo.protocolo, processo.id_unidade)

        if not documentos:
            raise HTTPException(status_code=404, detail="Nenhum documento encontrado")

        ultimo = documentos[-1]

        with ThreadPoolExecutor() as executor:
            fut_doc_ultimo = executor.submit(consultar_documento, token, processo.id_unidade, ultimo["DocumentoFormatado"])
            fut_md_ultimo = executor.submit(baixar_documento, token, processo.id_unidade, ultimo["DocumentoFormatado"])

            doc_ultimo = fut_doc_ultimo.result()
            md_ultimo = fut_md_ultimo.result()

            resposta_ia_ultimo = enviar_para_ia_conteudo(ler_arquivo_md(md_ultimo)) if md_ultimo else {}

        return {
            "status": "ok",
            "andamento": doc_ultimo,
            "resumo_ultimo": resposta_ia_ultimo,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/resumo/{numero_processo}")
def resumo(numero_processo: str):
    try:
        token = obter_token()
        processo = buscar_processo(numero_processo)
        documentos = listar_documentos(token, processo.protocolo, processo.id_unidade)

        if not documentos:
            raise HTTPException(status_code=404, detail="Nenhum documento encontrado")

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
        return {
            "status": "ok",
            "resumo": {
                "processo": processo.dict(),
                "primeiro_documento": doc_primeiro,
                "resumo_primeiro": resposta_ia_primeiro,
                "ultimo_documento": doc_ultimo,
                "resumo_ultimo": resposta_ia_ultimo
            }
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
