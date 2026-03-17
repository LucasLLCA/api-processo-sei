"""
Rotas para consulta de unidades SEI.
"""
import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models.unidade_sei import UnidadeSei

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/unidades-sei")
async def listar_unidades_sei(
    search: str = Query(default=None, description="Busca por sigla ou descrição"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    """
    Lista unidades SEI com busca e paginação.
    Sem search retorna paginado. Com search filtra por sigla ou descrição (case-insensitive).
    """
    base_query = select(UnidadeSei)
    count_query = select(func.count()).select_from(UnidadeSei)

    if search and search.strip():
        term = f"%{search.strip()}%"
        filter_clause = or_(
            UnidadeSei.sigla.ilike(term),
            UnidadeSei.descricao.ilike(term),
        )
        base_query = base_query.where(filter_clause)
        count_query = count_query.where(filter_clause)

    # Total count
    total_result = await db.execute(count_query)
    total = total_result.scalar() or 0

    # Paginated results
    offset = (page - 1) * page_size
    query = base_query.order_by(UnidadeSei.sigla).offset(offset).limit(page_size)
    result = await db.execute(query)
    rows = result.scalars().all()

    return {
        "items": [
            {
                "id_unidade": r.id_unidade,
                "sigla": r.sigla,
                "descricao": r.descricao,
            }
            for r in rows
        ],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 0,
    }
