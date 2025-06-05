from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from .routes import router
from .models import ErrorDetail, ErrorType
from .openai_client import client

app = FastAPI(
    title="API Processo SEI",
    description="API para consulta e análise de processos do SEI utilizando FastAPI e OpenAI",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://visualizadorprocessos.sei.sead.pi.gov.br",
        "https://api.sobdemanda.mandu.piaui.pro",
        "https://api.sei.agentes.sead.pi.gov.br"
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

@app.get("/")
def health_check():
    return {"status": "ok"}

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


