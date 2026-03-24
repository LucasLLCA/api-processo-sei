"""
Rotas para consulta de tipos de documento SEI.
"""
import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models.tipo_documento import TipoDocumento

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/documentos/tipos")
async def listar_tipos_documento(
    search: str = Query(default=None, description="Busca pelo nome do tipo de documento"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
):
    """
    Lista os tipos de documento disponíveis no SEI.
    Suporta busca por nome e paginação.
    """
    base_query = select(TipoDocumento)
    count_query = select(func.count()).select_from(TipoDocumento)

    if search and search.strip():
        term = f"%{search.strip()}%"
        base_query = base_query.where(TipoDocumento.nome.ilike(term))
        count_query = count_query.where(TipoDocumento.nome.ilike(term))

    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    offset = (page - 1) * page_size
    query = base_query.order_by(TipoDocumento.nome).offset(offset).limit(page_size)
    result = await db.execute(query)
    rows = result.scalars().all()

    return {
        "items": [
            {
                "id": r.id_tipo_documento,
                "nome": r.nome,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
    }
