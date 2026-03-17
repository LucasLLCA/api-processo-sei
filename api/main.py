import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse
from .routes import router
from .schemas_legacy import ErrorDetail, ErrorType
from .config import settings
from .cache import cache
from .openai_client import client
from .database import close_db, engine, Base

# Configurar logging estruturado
# Note: otelTraceID/otelSpanID are injected into log records by OTEL logging
# instrumentation automatically. We keep the console format simple to avoid
# KeyError when fields aren't yet available (e.g., during startup).
# Trace correlation still works in SigNoz via the OTEL LoggingHandler.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

# Initialize OpenTelemetry before FastAPI app creation
from .telemetry import configure_telemetry
configure_telemetry()

logger = logging.getLogger(__name__)


def _run_alembic_upgrade():
    """Executa alembic upgrade head de forma síncrona"""
    from alembic.config import Config
    from alembic import command

    alembic_ini = Path(__file__).resolve().parents[1] / "alembic.ini"
    if not alembic_ini.exists():
        logger.warning(f"alembic.ini não encontrado em {alembic_ini}, pulando migrations")
        return

    alembic_cfg = Config(str(alembic_ini))
    command.upgrade(alembic_cfg, "head")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gerencia o ciclo de vida da aplicação"""
    # Startup
    logger.info("Iniciando API Processo SEI...")

    # Run Alembic migrations
    if not settings.SKIP_MIGRATIONS:
        try:
            logger.info("Executando migrations do banco de dados...")
            await asyncio.to_thread(_run_alembic_upgrade)
            logger.info("Migrations executadas com sucesso")
        except Exception as e:
            logger.error(f"Erro ao executar migrations: {e}")
    

    # Ensure new tables/columns exist (safe for existing DBs)
    try:
        # Import all models so metadata is complete
        from . import models  # noqa: F401
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            # Add papel_global column if missing (existing table)
            await conn.execute(
                __import__('sqlalchemy').text(
                    "ALTER TABLE credenciais_usuario ADD COLUMN IF NOT EXISTS "
                    "papel_global VARCHAR(20) NOT NULL DEFAULT 'user'"
                )
            )
        logger.info("Schema do banco de dados atualizado")
    except Exception as e:
        logger.warning(f"Erro ao atualizar schema (pode já estar atualizado): {e}")

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

# OpenTelemetry FastAPI auto-instrumentation
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
FastAPIInstrumentor.instrument_app(app)

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
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
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
