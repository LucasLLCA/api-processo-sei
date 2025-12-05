from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore"
    }
    
    API_TITLE: str = "API Processo SEI"
    API_DESCRIPTION: str = "API para consulta e análise de processos do SEI utilizando FastAPI e OpenAI"
    API_VERSION: str = "1.0.0"
    API_PORT: int = 8535
    API_HOST: str = "0.0.0.0"

    SEI_BASE_URL: str = "https://api.sei.pi.gov.br/v1"

    OPENAI_BASE_URL: str = "https://api.sobdemanda.mandu.piaui.pro"
    OPENAI_API_KEY: str = "sk-Thp8OzZ_6U3tHoDStt8qYg"
    OPENAI_MODEL: str = "Qwen/Qwen3-30B-A3B"

    # Configurações Redis
    REDIS_HOST: str = "redis"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_USERNAME: str = "default"
    REDIS_PASSWORD: str = ""

    # Configurações PostgreSQL
    DATABASE_HOST: str = "localhost"
    DATABASE_PORT: int = 5432
    DATABASE_USER: str = "postgres"
    DATABASE_PASSWORD: str = ""
    DATABASE_NAME: str = "postgres"
    DATABASE_ECHO: bool = False
    DATABASE_POOL_SIZE: int = 5
    DATABASE_MAX_OVERFLOW: int = 10

    @property
    def DATABASE_URL(self) -> str:
        """Constrói a URL de conexão do PostgreSQL para asyncpg"""
        return (
            f"postgresql+asyncpg://{self.DATABASE_USER}:{self.DATABASE_PASSWORD}"
            f"@{self.DATABASE_HOST}:{self.DATABASE_PORT}/{self.DATABASE_NAME}"
        )


settings = Settings()