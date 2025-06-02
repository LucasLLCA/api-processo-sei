from fastapi import APIRouter
from .processo import router as processo_router

router = APIRouter()

router.include_router(processo_router, prefix="/processo", tags=["Processo"]) 