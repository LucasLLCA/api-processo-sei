from fastapi import APIRouter
from .processo import router as processo_router
from .admin import router as admin_router
from .historico import router as historico_router

router = APIRouter()

router.include_router(processo_router, prefix="/processo", tags=["Processo"])
router.include_router(admin_router, prefix="/admin", tags=["Admin"])
router.include_router(historico_router, prefix="/historico", tags=["Histórico"]) 