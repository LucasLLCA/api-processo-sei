"""
Rotas para gerenciamento de observacoes sobre processos
"""
import re
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from uuid import UUID
import logging

from ..database import get_db
from ..models import Observacao, EquipeMembro
from ..schemas import ObservacaoCreate, ObservacaoResponse

router = APIRouter()
logger = logging.getLogger(__name__)


def _strip_non_digits(value: str) -> str:
    return re.sub(r'\D', '', value)


@router.get(
    "/{numero_processo}",
    response_model=dict,
    summary="Listar observacoes de um processo",
)
async def listar_observacoes(
    numero_processo: str,
    equipe_id: UUID | None = Query(None, description="Filtrar por equipe (NULL = global)"),
    usuario: str | None = Query(None, description="Usuario (obrigatorio se equipe_id)"),
    db: AsyncSession = Depends(get_db),
):
    try:
        numero_limpo = _strip_non_digits(numero_processo)

        # Se equipe_id fornecido, verificar membership
        if equipe_id is not None:
            if not usuario:
                raise HTTPException(status_code=400, detail="usuario e obrigatorio para observacoes de equipe")
            membro_q = await db.execute(
                select(EquipeMembro).where(and_(
                    EquipeMembro.equipe_id == equipe_id,
                    EquipeMembro.usuario == usuario,
                    EquipeMembro.deletado_em.is_(None),
                ))
            )
            if not membro_q.scalar_one_or_none():
                raise HTTPException(status_code=403, detail="Voce nao e membro desta equipe")

        conditions = [
            Observacao.numero_processo == numero_limpo,
            Observacao.deletado_em.is_(None),
        ]
        if equipe_id is not None:
            conditions.append(Observacao.equipe_id == equipe_id)
        else:
            conditions.append(Observacao.equipe_id.is_(None))

        query = (
            select(Observacao)
            .where(and_(*conditions))
            .order_by(Observacao.criado_em.asc())
        )
        result = await db.execute(query)
        observacoes = result.scalars().all()

        return {
            "status": "success",
            "data": [ObservacaoResponse.model_validate(o) for o in observacoes],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao listar observacoes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/{numero_processo}",
    response_model=dict,
    status_code=201,
    summary="Criar observacao sobre um processo",
)
async def criar_observacao(
    numero_processo: str,
    dados: ObservacaoCreate,
    usuario: str = Query(..., description="Usuario autor"),
    db: AsyncSession = Depends(get_db),
):
    try:
        numero_limpo = _strip_non_digits(numero_processo)

        # Se equipe_id fornecido, verificar membership
        if dados.equipe_id is not None:
            membro_q = await db.execute(
                select(EquipeMembro).where(and_(
                    EquipeMembro.equipe_id == dados.equipe_id,
                    EquipeMembro.usuario == usuario,
                    EquipeMembro.deletado_em.is_(None),
                ))
            )
            if not membro_q.scalar_one_or_none():
                raise HTTPException(status_code=403, detail="Voce nao e membro desta equipe")

        observacao = Observacao(
            numero_processo=numero_limpo,
            usuario=usuario,
            conteudo=dados.conteudo,
            equipe_id=dados.equipe_id,
        )
        db.add(observacao)
        await db.commit()
        await db.refresh(observacao)

        logger.info(f"Observacao criada: processo={numero_limpo}, usuario={usuario}")

        return {
            "status": "success",
            "data": ObservacaoResponse.model_validate(observacao),
        }
    except Exception as e:
        logger.error(f"Erro ao criar observacao: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{numero_processo}/{observacao_id}",
    response_model=dict,
    summary="Excluir observacao (soft delete)",
)
async def deletar_observacao(
    numero_processo: str,
    observacao_id: UUID,
    usuario: str = Query(..., description="Usuario autor"),
    db: AsyncSession = Depends(get_db),
):
    try:
        numero_limpo = _strip_non_digits(numero_processo)
        result = await db.execute(
            select(Observacao).where(and_(
                Observacao.id == observacao_id,
                Observacao.numero_processo == numero_limpo,
                Observacao.deletado_em.is_(None),
            ))
        )
        observacao = result.scalar_one_or_none()
        if not observacao:
            raise HTTPException(status_code=404, detail="Observacao nao encontrada")

        if observacao.usuario != usuario:
            raise HTTPException(status_code=403, detail="Apenas o autor pode excluir a observacao")

        observacao.soft_delete()
        await db.commit()

        return {"status": "success", "message": "Observacao excluida com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar observacao: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
