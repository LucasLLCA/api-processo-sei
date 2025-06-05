from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import List

class Settings(BaseSettings):
    # Configurações da API
    API_TITLE: str = "API Processo SEI"
    API_DESCRIPTION: str = "API para consulta e análise de processos do SEI utilizando FastAPI e OpenAI"
    API_VERSION: str = "1.0.0"
    API_PORT: int = 8443
    API_HOST: str = "0.0.0.0"

    # Configurações de CORS
    CORS_ORIGINS: List[str] = [
        "https://visualizadorprocessos.sei.sead.pi.gov.br",
        "https://api.sobdemanda.mandu.piaui.pro",
        "https://api.sei.agentes.sead.pi.gov.br"
    ]

    # Configurações do Banco de Dados
    DB_HOST: str
    DB_PORT: str
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str

    # Configurações do SEI
    SEAD_USUARIO: str
    SEAD_SENHA: str
    SEAD_ORGAO: str
    SEI_BASE_URL: str = "https://api.sead.pi.gov.br/sei/v1"

    # Configurações da OpenAI
    OPENAI_BASE_URL: str
    OPENAI_API_KEY: str
    OPENAI_MODEL: str = "Qwen/Qwen3-30B-A3B"

    class Config:
        env_file = ".env"
        case_sensitive = True

@lru_cache()
def get_settings() -> Settings:
    return Settings()

# Configurações do banco de dados
DB_CONFIG = {
    "host": get_settings().DB_HOST,
    "port": get_settings().DB_PORT,
    "user": get_settings().DB_USER,
    "password": get_settings().DB_PASSWORD,
    "dbname": get_settings().DB_NAME
}

# Credenciais do SEI
SEI_CREDENTIALS = {
    "usuario": get_settings().SEAD_USUARIO,
    "senha": get_settings().SEAD_SENHA,
    "orgao": get_settings().SEAD_ORGAO
}
