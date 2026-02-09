import logging
import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse
from .routes import router
from .schemas_legacy import ErrorDetail, ErrorType
from .config import settings
from .cache import cache
from .openai_client import client
from .database import close_db

# Configurar logging estruturado
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia o ciclo de vida da aplicação"""
    # Startup
    logger.info("Iniciando API Processo SEI...")
    await cache.connect()
    logger.info("Cache conectado")
    logger.info("API iniciada com sucesso")

    yield

    # Shutdown
    logger.info("Encerrando API...")
    await cache.close()
    logger.info("Cache desconectado")
    await close_db()
    logger.info("Banco de dados desconectado")
    logger.info("API encerrada")


app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION,
    default_response_class=ORJSONResponse,
    lifespan=lifespan
)

# Middleware GZip para compressão
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Middleware CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://sei.pi.gov.br",
        "https://visualizadorprocessos.sei.sead.pi.gov.br",
        "http://visualizadorprocessos.sei.sead.pi.gov.br",
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "PATCH"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID", "X-SEI-Token", "x-api-key"],
)


@app.get("/")
async def health_check():
    return {
        "status": "ok",
        "version": settings.API_VERSION,
        "environment": "production"
    }


@app.get("/test-ia")
async def test_ia():
    try:
        resposta = await client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Você é um assistente de teste."},
                {"role": "user", "content": "Responda apenas com 'Teste OK'"}
            ],
            temperature=0.7,
        )
        logger.debug(f"Resposta do teste IA: {resposta}")
        if not resposta.choices or len(resposta.choices) == 0:
            raise ValueError("Resposta vazia do modelo OpenAI")
        return {"status": "ok", "message": resposta.choices[0].message.content.strip()}
    except Exception as e:
        logger.error(f"Erro no teste IA: {str(e)}")
        return {"status": "erro", "message": str(e)}


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Erro não tratado: {str(exc)}", exc_info=True)
    error_detail = ErrorDetail(
        type=ErrorType.PROCESSING_ERROR,
        message="Erro interno do servidor",
        details={"error": str(exc)}
    )
    return ORJSONResponse(
        status_code=500,
        content={"status": "error", "error": error_detail.dict()}
    )


app.include_router(router)
