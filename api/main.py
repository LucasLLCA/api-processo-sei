from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from .routes import router
import logging
import sys
from datetime import datetime

# Configuração do logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('api.log')
    ]
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="API Processo SEI",
    description="API para consulta e análise de processos do SEI utilizando FastAPI e OpenAI",
    version="1.0.0"
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = datetime.now()
    try:
        response = await call_next(request)
        process_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Request: {request.method} {request.url.path} - Status: {response.status_code} - Tempo: {process_time:.2f}s")
        return response
    except Exception as e:
        logger.error(f"Erro na requisição {request.method} {request.url.path}: {str(e)}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Erro interno do servidor"}
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


