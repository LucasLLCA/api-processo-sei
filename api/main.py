from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from .routes import router
from .models import ErrorDetail, ErrorType
from .config import settings
from .openai_client import client

# Cria a aplicação FastAPI
app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION
)

# Configura o CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Endpoint de health check
@app.get("/")
def health_check():
    return {
        "status": "ok",
        "version": settings.API_VERSION,
        "environment": "production"
    }

@app.get("/test-ia")
async def test_ia():
    try:
        # Tenta fazer uma requisição simples para a API da IA
        resposta = client.chat.completions.create(
            model="Qwen/Qwen3-30B-A3B",
            messages=[
                {"role": "system", "content": "Você é um assistente de teste."},
                {"role": "user", "content": "Responda apenas com 'Teste OK'"}
            ],
            temperature=0.7,
        )
        return {"status": "ok", "message": resposta.choices[0].message.content.strip()}
    except Exception as e:
        return {"status": "erro", "message": str(e)}

# Handler global de exceções
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    error_detail = ErrorDetail(
        type=ErrorType.PROCESSING_ERROR,
        message="Erro interno do servidor",
        details={"error": str(exc)}
    )
    return JSONResponse(
        status_code=500,
        content={"status": "error", "error": error_detail.dict()}
    )

# Inclui as rotas
app.include_router(router)

