from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from .routes import router
from .models import ErrorDetail, ErrorType
from .config import settings
from .openai_client import client

app = FastAPI(
    title=settings.API_TITLE,
    description=settings.API_DESCRIPTION,
    version=settings.API_VERSION
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://visualizadorprocessos.sei.sead.pi.gov.br/",
        "https://api.sobdemanda.mandu.piaui.pro/v1",
        "http://api.sei.agentes.sead.pi.gov.br",
        "http://localhost:9002",
        "http://localhost:3000"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
        resposta = client.chat.completions.create(
            model=settings.OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "Você é um assistente de teste."},
                {"role": "user", "content": "Responda apenas com 'Teste OK'"}
            ],
            temperature=0.7,
        )
        print(resposta)  # Log da resposta para depuração
        if not resposta.choices or len(resposta.choices) == 0:
            raise ValueError("Resposta vazia do modelo OpenAI")
        return {"status": "ok", "message": resposta.choices[0].message.content.strip()}
    except Exception as e:
        return {"status": "erro", "message": str(e)}

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

app.include_router(router)
