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
    API_PORT: int = 8443
    API_HOST: str = "0.0.0.0"



    DB_HOST: str
    DB_PORT: str
    DB_USER: str
    DB_PASSWORD: str
    DB_NAME: str

    SEAD_USUARIO: str
    SEAD_SENHA: str
    SEAD_ORGAO: str
    SEI_BASE_URL: str = "https://api.sead.pi.gov.br/sei/v1"

    OPENAI_BASE_URL: str
    OPENAI_API_KEY: str
    OPENAI_MODEL: str = "Qwen/Qwen3-30B-A3B"

    @property
    def DB_CONFIG(self):
        return {
            "host": self.DB_HOST,
            "port": self.DB_PORT,
            "user": self.DB_USER,
            "password": self.DB_PASSWORD,
            "dbname": self.DB_NAME
        }

settings = Settings()
