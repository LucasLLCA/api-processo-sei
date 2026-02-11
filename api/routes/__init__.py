from fastapi import APIRouter
from .processo import router as processo_router
from .admin import router as admin_router
from .historico import router as historico_router
from .auth import router as auth_router
from .sei_proxy import router as sei_proxy_router

router = APIRouter()

router.include_router(processo_router, prefix="/processo", tags=["Processo"])
router.include_router(admin_router, prefix="/admin", tags=["Admin"])
router.include_router(historico_router, prefix="/historico", tags=["Histórico"])
router.include_router(auth_router, prefix="/auth", tags=["Auth"])
router.include_router(sei_proxy_router, prefix="/sei", tags=["SEI Proxy"])