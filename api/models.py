from pydantic import BaseModel

class Processo(BaseModel):
    numero: str
    protocolo: str
    id_unidade: str
    assunto: str

class Documento(BaseModel):
    documento_formatado: str

class DocumentoDetalhado(BaseModel):
    conteudo: str
    titulo: str

class Retorno(BaseModel):
    status: str
    resumo: dict | None = None
    andamento: dict | None = None

class Andamentos(BaseModel):
    andamento: str
