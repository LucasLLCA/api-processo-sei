from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import router

app = FastAPI(
    title="API Processo SEI",
    description="API para consulta e análise de processos do SEI utilizando FastAPI e OpenAI",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://visualizadorprocessos.sei.sead.pi.gov.br",
        "https://api.sei.agentes.sead.pi.gov.br"
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)


