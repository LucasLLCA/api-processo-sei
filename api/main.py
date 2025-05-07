from fastapi import FastAPI, HTTPException
from .sei import obter_token, buscar_processo, listar_documentos, listar_tarefa, consultar_documento, baixar_documento
from .openai_client import enviar_para_ia

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
        doc_ultimo = consultar_documento(token, processo.id_unidade, ultimo["DocumentoFormatado"])
        caminho_md_ultimo = baixar_documento(token, processo.id_unidade, ultimo["DocumentoFormatado"])
        resposta_ia_ultimo = enviar_para_ia(caminho_md_ultimo) if caminho_md_ultimo else {}

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
        andamentos = listar_tarefa(token, processo.protocolo, processo.id_unidade)

        if not documentos:
            raise HTTPException(status_code=404, detail="Nenhum documento encontrado")

        primeiro = documentos[0]
        ultimo = documentos[-1]

        doc_primeiro = consultar_documento(token, processo.id_unidade, primeiro["DocumentoFormatado"])
        doc_ultimo = consultar_documento(token, processo.id_unidade, ultimo["DocumentoFormatado"])

        caminho_md_primeiro = baixar_documento(token, processo.id_unidade, primeiro["DocumentoFormatado"])
        caminho_md_ultimo = baixar_documento(token, processo.id_unidade, ultimo["DocumentoFormatado"])

        resposta_ia_primeiro = enviar_para_ia(caminho_md_primeiro) if caminho_md_primeiro else {}
        resposta_ia_ultimo = enviar_para_ia(caminho_md_ultimo) if caminho_md_ultimo else {}

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
