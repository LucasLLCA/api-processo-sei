"""
Rotas para gerenciamento de equipes
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from datetime import datetime
from uuid import UUID
import logging

from ..database import get_db
from ..models import Equipe, EquipeMembro
from ..schemas import (
    EquipeCreate,
    EquipeUpdate,
    MembroAdd,
    MembroResponse,
    EquipeResponse,
    EquipeDetalheResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post(
    "",
    response_model=dict,
    status_code=201,
    summary="Criar equipe",
)
async def criar_equipe(
    dados: EquipeCreate,
    usuario: str = Query(..., description="Usuário criador da equipe"),
    db: AsyncSession = Depends(get_db),
):
    try:
        equipe = Equipe(
            nome=dados.nome,
            descricao=dados.descricao,
            proprietario_usuario=usuario,
        )
        db.add(equipe)
        await db.flush()

        # Adicionar criador como admin
        membro = EquipeMembro(
            equipe_id=equipe.id,
            usuario=usuario,
            papel="admin",
        )
        db.add(membro)
        await db.commit()
        await db.refresh(equipe)

        logger.info(f"Equipe criada: nome={dados.nome}, proprietario={usuario}")

        return {
            "status": "success",
            "data": EquipeResponse(
                id=equipe.id,
                nome=equipe.nome,
                descricao=equipe.descricao,
                proprietario_usuario=equipe.proprietario_usuario,
                criado_em=equipe.criado_em,
                atualizado_em=equipe.atualizado_em,
                total_membros=1,
            ),
        }
    except Exception as e:
        logger.error(f"Erro ao criar equipe: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "",
    response_model=dict,
    summary="Listar equipes do usuário",
)
async def listar_equipes(
    usuario: str = Query(..., description="Usuário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        # Equipes onde o usuário é membro (não deletado)
        subq_membros = (
            select(EquipeMembro.equipe_id)
            .where(and_(
                EquipeMembro.usuario == usuario,
                EquipeMembro.deletado_em.is_(None),
            ))
        )

        query = (
            select(Equipe)
            .where(and_(
                Equipe.id.in_(subq_membros),
                Equipe.deletado_em.is_(None),
            ))
            .order_by(Equipe.criado_em.desc())
        )

        result = await db.execute(query)
        equipes = result.scalars().all()

        equipes_response = []
        for eq in equipes:
            # Contar membros ativos
            count_q = select(func.count()).select_from(EquipeMembro).where(
                and_(
                    EquipeMembro.equipe_id == eq.id,
                    EquipeMembro.deletado_em.is_(None),
                )
            )
            count_result = await db.execute(count_q)
            total = count_result.scalar()

            equipes_response.append(EquipeResponse(
                id=eq.id,
                nome=eq.nome,
                descricao=eq.descricao,
                proprietario_usuario=eq.proprietario_usuario,
                criado_em=eq.criado_em,
                atualizado_em=eq.atualizado_em,
                total_membros=total,
            ))

        return {"status": "success", "data": equipes_response}

    except Exception as e:
        logger.error(f"Erro ao listar equipes: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{equipe_id}",
    response_model=dict,
    summary="Detalhes da equipe",
)
async def detalhe_equipe(
    equipe_id: UUID,
    usuario: str = Query(..., description="Usuário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        equipe = await _get_equipe_como_membro(db, equipe_id, usuario)

        membros_q = select(EquipeMembro).where(
            and_(
                EquipeMembro.equipe_id == equipe_id,
                EquipeMembro.deletado_em.is_(None),
            )
        )
        result = await db.execute(membros_q)
        membros = result.scalars().all()

        return {
            "status": "success",
            "data": EquipeDetalheResponse(
                id=equipe.id,
                nome=equipe.nome,
                descricao=equipe.descricao,
                proprietario_usuario=equipe.proprietario_usuario,
                criado_em=equipe.criado_em,
                atualizado_em=equipe.atualizado_em,
                membros=[MembroResponse.model_validate(m) for m in membros],
            ),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar equipe: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch(
    "/{equipe_id}",
    response_model=dict,
    summary="Atualizar equipe",
)
async def atualizar_equipe(
    equipe_id: UUID,
    dados: EquipeUpdate,
    usuario: str = Query(..., description="Usuário proprietário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        equipe = await _get_equipe_como_proprietario(db, equipe_id, usuario)

        if dados.nome is not None:
            equipe.nome = dados.nome
        if dados.descricao is not None:
            equipe.descricao = dados.descricao
        equipe.atualizado_em = datetime.utcnow()

        await db.commit()
        await db.refresh(equipe)

        return {"status": "success", "data": EquipeResponse.model_validate(equipe)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao atualizar equipe: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{equipe_id}",
    response_model=dict,
    summary="Excluir equipe",
)
async def deletar_equipe(
    equipe_id: UUID,
    usuario: str = Query(..., description="Usuário proprietário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        equipe = await _get_equipe_como_proprietario(db, equipe_id, usuario)
        equipe.soft_delete()
        await db.commit()

        return {"status": "success", "message": "Equipe excluída com sucesso"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar equipe: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/{equipe_id}/membros",
    response_model=dict,
    status_code=201,
    summary="Adicionar membro à equipe",
)
async def adicionar_membro(
    equipe_id: UUID,
    dados: MembroAdd,
    usuario: str = Query(..., description="Usuário admin da equipe"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _verificar_admin(db, equipe_id, usuario)

        # Verificar se já é membro
        existente = await db.execute(
            select(EquipeMembro).where(and_(
                EquipeMembro.equipe_id == equipe_id,
                EquipeMembro.usuario == dados.usuario,
                EquipeMembro.deletado_em.is_(None),
            ))
        )
        if existente.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Usuário já é membro da equipe")

        membro = EquipeMembro(
            equipe_id=equipe_id,
            usuario=dados.usuario,
            papel=dados.papel,
        )
        db.add(membro)
        await db.commit()
        await db.refresh(membro)

        return {
            "status": "success",
            "data": MembroResponse.model_validate(membro),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao adicionar membro: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{equipe_id}/membros/{membro_usuario}",
    response_model=dict,
    summary="Remover membro da equipe",
)
async def remover_membro(
    equipe_id: UUID,
    membro_usuario: str,
    usuario: str = Query(..., description="Usuário solicitante"),
    db: AsyncSession = Depends(get_db),
):
    try:
        # Pode remover se é admin ou se está removendo a si mesmo
        if membro_usuario != usuario:
            await _verificar_admin(db, equipe_id, usuario)

        result = await db.execute(
            select(EquipeMembro).where(and_(
                EquipeMembro.equipe_id == equipe_id,
                EquipeMembro.usuario == membro_usuario,
                EquipeMembro.deletado_em.is_(None),
            ))
        )
        membro = result.scalar_one_or_none()
        if not membro:
            raise HTTPException(status_code=404, detail="Membro não encontrado")

        membro.soft_delete()
        await db.commit()

        return {"status": "success", "message": "Membro removido com sucesso"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao remover membro: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# --- Helpers ---

async def _get_equipe_como_membro(db: AsyncSession, equipe_id: UUID, usuario: str) -> Equipe:
    result = await db.execute(
        select(Equipe).where(and_(Equipe.id == equipe_id, Equipe.deletado_em.is_(None)))
    )
    equipe = result.scalar_one_or_none()
    if not equipe:
        raise HTTPException(status_code=404, detail="Equipe não encontrada")

    membro_q = await db.execute(
        select(EquipeMembro).where(and_(
            EquipeMembro.equipe_id == equipe_id,
            EquipeMembro.usuario == usuario,
            EquipeMembro.deletado_em.is_(None),
        ))
    )
    if not membro_q.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Você não é membro desta equipe")

    return equipe


async def _get_equipe_como_proprietario(db: AsyncSession, equipe_id: UUID, usuario: str) -> Equipe:
    result = await db.execute(
        select(Equipe).where(and_(Equipe.id == equipe_id, Equipe.deletado_em.is_(None)))
    )
    equipe = result.scalar_one_or_none()
    if not equipe:
        raise HTTPException(status_code=404, detail="Equipe não encontrada")
    if equipe.proprietario_usuario != usuario:
        raise HTTPException(status_code=403, detail="Apenas o proprietário pode realizar esta ação")
    return equipe


async def _verificar_admin(db: AsyncSession, equipe_id: UUID, usuario: str):
    # Verificar que equipe existe
    eq = await db.execute(
        select(Equipe).where(and_(Equipe.id == equipe_id, Equipe.deletado_em.is_(None)))
    )
    if not eq.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Equipe não encontrada")

    result = await db.execute(
        select(EquipeMembro).where(and_(
            EquipeMembro.equipe_id == equipe_id,
            EquipeMembro.usuario == usuario,
            EquipeMembro.papel == "admin",
            EquipeMembro.deletado_em.is_(None),
        ))
    )
    if not result.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Apenas administradores podem realizar esta ação")
