from pydantic import BaseModel
from enum import Enum

class ErrorType(str, Enum):
    AUTHENTICATION_ERROR = "authentication_error"
    NOT_FOUND = "not_found"
    VALIDATION_ERROR = "validation_error"
    PROCESSING_ERROR = "processing_error"
    EXTERNAL_SERVICE_ERROR = "external_service_error"
    DATABASE_ERROR = "database_error"

class ErrorDetail(BaseModel):
    type: ErrorType
    message: str
    details: dict | None = None

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
    error: ErrorDetail | None = None

class Andamentos(BaseModel):
    andamento: str
