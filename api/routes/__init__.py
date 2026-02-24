from fastapi import APIRouter
from .processo import router as processo_router
from .admin import router as admin_router
from .historico import router as historico_router
from .auth import router as auth_router
from .sei_proxy import router as sei_proxy_router
from .equipes import router as equipes_router
from .tags import router as tags_router
from .compartilhamentos import router as compartilhamentos_router
from .observacoes import router as observacoes_router
from .team_tags import router as team_tags_router

router = APIRouter()

router.include_router(processo_router, prefix="/processo", tags=["Processo"])
router.include_router(admin_router, prefix="/admin", tags=["Admin"])
router.include_router(historico_router, prefix="/historico", tags=["Histórico"])
router.include_router(auth_router, prefix="/auth", tags=["Auth"])
router.include_router(sei_proxy_router, prefix="/sei", tags=["SEI Proxy"])
router.include_router(equipes_router, prefix="/equipes", tags=["Equipes"])
router.include_router(tags_router, prefix="/tags", tags=["Tags"])
router.include_router(compartilhamentos_router, prefix="/compartilhamentos", tags=["Compartilhamentos"])
router.include_router(observacoes_router, prefix="/observacoes", tags=["Observacoes"])
router.include_router(team_tags_router, prefix="/equipes", tags=["Team Tags"])
