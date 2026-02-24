"""
Rotas para gerenciamento de tags de equipe (rotulos no kanban)
"""
import re
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from datetime import datetime
from uuid import UUID
import logging

from ..database import get_db
from ..models import TeamTag, ProcessoTeamTag, Equipe, EquipeMembro
from ..schemas import (
    TeamTagCreate,
    TeamTagUpdate,
    TeamTagResponse,
    ProcessoTeamTagCreate,
    ProcessoTeamTagResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def _strip_non_digits(value: str) -> str:
    return re.sub(r'\D', '', value)


async def _verificar_membro(db: AsyncSession, equipe_id: UUID, usuario: str):
    """Verifica que a equipe existe e o usuario e membro."""
    eq = await db.execute(
        select(Equipe).where(and_(Equipe.id == equipe_id, Equipe.deletado_em.is_(None)))
    )
    if not eq.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Equipe nao encontrada")

    membro = await db.execute(
        select(EquipeMembro).where(and_(
            EquipeMembro.equipe_id == equipe_id,
            EquipeMembro.usuario == usuario,
            EquipeMembro.deletado_em.is_(None),
        ))
    )
    if not membro.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Voce nao e membro desta equipe")


@router.post(
    "/{equipe_id}/tags",
    response_model=dict,
    status_code=201,
    summary="Criar tag de equipe",
)
async def criar_team_tag(
    equipe_id: UUID,
    dados: TeamTagCreate,
    usuario: str = Query(..., description="Usuario criador"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _verificar_membro(db, equipe_id, usuario)

        # Verificar duplicata
        existente = await db.execute(
            select(TeamTag).where(and_(
                TeamTag.equipe_id == equipe_id,
                TeamTag.nome == dados.nome,
                TeamTag.deletado_em.is_(None),
            ))
        )
        if existente.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Ja existe uma tag com este nome nesta equipe")

        tag = TeamTag(
            equipe_id=equipe_id,
            nome=dados.nome,
            cor=dados.cor,
            criado_por=usuario,
        )
        db.add(tag)
        await db.commit()
        await db.refresh(tag)

        logger.info(f"Team tag criada: equipe={equipe_id}, nome={dados.nome}, por={usuario}")

        return {
            "status": "success",
            "data": TeamTagResponse.model_validate(tag),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao criar team tag: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{equipe_id}/tags",
    response_model=dict,
    summary="Listar tags da equipe",
)
async def listar_team_tags(
    equipe_id: UUID,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _verificar_membro(db, equipe_id, usuario)

        query = (
            select(TeamTag)
            .where(and_(
                TeamTag.equipe_id == equipe_id,
                TeamTag.deletado_em.is_(None),
            ))
            .order_by(TeamTag.nome.asc())
        )
        result = await db.execute(query)
        tags = result.scalars().all()

        return {
            "status": "success",
            "data": [TeamTagResponse.model_validate(t) for t in tags],
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao listar team tags: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch(
    "/{equipe_id}/tags/{tag_id}",
    response_model=dict,
    summary="Atualizar tag de equipe",
)
async def atualizar_team_tag(
    equipe_id: UUID,
    tag_id: UUID,
    dados: TeamTagUpdate,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _verificar_membro(db, equipe_id, usuario)

        result = await db.execute(
            select(TeamTag).where(and_(
                TeamTag.id == tag_id,
                TeamTag.equipe_id == equipe_id,
                TeamTag.deletado_em.is_(None),
            ))
        )
        tag = result.scalar_one_or_none()
        if not tag:
            raise HTTPException(status_code=404, detail="Tag nao encontrada")

        if dados.nome is not None:
            # Verificar duplicata do novo nome
            dup = await db.execute(
                select(TeamTag).where(and_(
                    TeamTag.equipe_id == equipe_id,
                    TeamTag.nome == dados.nome,
                    TeamTag.id != tag_id,
                    TeamTag.deletado_em.is_(None),
                ))
            )
            if dup.scalar_one_or_none():
                raise HTTPException(status_code=409, detail="Ja existe uma tag com este nome nesta equipe")
            tag.nome = dados.nome

        if dados.cor is not None:
            tag.cor = dados.cor

        tag.atualizado_em = datetime.utcnow()
        await db.commit()
        await db.refresh(tag)

        return {
            "status": "success",
            "data": TeamTagResponse.model_validate(tag),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao atualizar team tag: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{equipe_id}/tags/{tag_id}",
    response_model=dict,
    summary="Excluir tag de equipe (soft delete)",
)
async def deletar_team_tag(
    equipe_id: UUID,
    tag_id: UUID,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _verificar_membro(db, equipe_id, usuario)

        result = await db.execute(
            select(TeamTag).where(and_(
                TeamTag.id == tag_id,
                TeamTag.equipe_id == equipe_id,
                TeamTag.deletado_em.is_(None),
            ))
        )
        tag = result.scalar_one_or_none()
        if not tag:
            raise HTTPException(status_code=404, detail="Tag nao encontrada")

        tag.soft_delete()
        await db.commit()

        return {"status": "success", "message": "Tag excluida com sucesso"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar team tag: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/{equipe_id}/tags/{tag_id}/processos",
    response_model=dict,
    status_code=201,
    summary="Associar tag a um processo",
)
async def tag_processo(
    equipe_id: UUID,
    tag_id: UUID,
    dados: ProcessoTeamTagCreate,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _verificar_membro(db, equipe_id, usuario)

        # Verificar que a tag pertence a equipe
        tag_q = await db.execute(
            select(TeamTag).where(and_(
                TeamTag.id == tag_id,
                TeamTag.equipe_id == equipe_id,
                TeamTag.deletado_em.is_(None),
            ))
        )
        if not tag_q.scalar_one_or_none():
            raise HTTPException(status_code=404, detail="Tag nao encontrada")

        numero_limpo = _strip_non_digits(dados.numero_processo)

        # Verificar duplicata
        existente = await db.execute(
            select(ProcessoTeamTag).where(and_(
                ProcessoTeamTag.team_tag_id == tag_id,
                ProcessoTeamTag.numero_processo == numero_limpo,
                ProcessoTeamTag.deletado_em.is_(None),
            ))
        )
        if existente.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Este processo ja possui esta tag")

        assoc = ProcessoTeamTag(
            team_tag_id=tag_id,
            numero_processo=numero_limpo,
            adicionado_por=usuario,
        )
        db.add(assoc)
        await db.commit()
        await db.refresh(assoc)

        return {
            "status": "success",
            "data": ProcessoTeamTagResponse.model_validate(assoc),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao associar tag ao processo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{equipe_id}/tags/{tag_id}/processos/{processo_tag_id}",
    response_model=dict,
    summary="Remover tag de um processo",
)
async def untag_processo(
    equipe_id: UUID,
    tag_id: UUID,
    processo_tag_id: UUID,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _verificar_membro(db, equipe_id, usuario)

        result = await db.execute(
            select(ProcessoTeamTag).where(and_(
                ProcessoTeamTag.id == processo_tag_id,
                ProcessoTeamTag.team_tag_id == tag_id,
                ProcessoTeamTag.deletado_em.is_(None),
            ))
        )
        assoc = result.scalar_one_or_none()
        if not assoc:
            raise HTTPException(status_code=404, detail="Associacao nao encontrada")

        assoc.soft_delete()
        await db.commit()

        return {"status": "success", "message": "Tag removida do processo"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao remover tag do processo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{equipe_id}/tags/por-processo/{numero_processo}",
    response_model=dict,
    summary="Tags de equipe de um processo",
)
async def tags_por_processo(
    equipe_id: UUID,
    numero_processo: str,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _verificar_membro(db, equipe_id, usuario)

        numero_limpo = _strip_non_digits(numero_processo)

        query = (
            select(ProcessoTeamTag)
            .join(TeamTag, ProcessoTeamTag.team_tag_id == TeamTag.id)
            .where(and_(
                ProcessoTeamTag.numero_processo == numero_limpo,
                ProcessoTeamTag.deletado_em.is_(None),
                TeamTag.equipe_id == equipe_id,
                TeamTag.deletado_em.is_(None),
            ))
        )
        result = await db.execute(query)
        assocs = result.scalars().all()

        # Buscar as tags correspondentes
        tag_ids = [a.team_tag_id for a in assocs]
        tags_data = []
        if tag_ids:
            tags_q = await db.execute(
                select(TeamTag).where(and_(
                    TeamTag.id.in_(tag_ids),
                    TeamTag.deletado_em.is_(None),
                ))
            )
            tags_data = [TeamTagResponse.model_validate(t) for t in tags_q.scalars().all()]

        return {"status": "success", "data": tags_data}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar tags por processo: {e}")
        raise HTTPException(status_code=500, detail=str(e))
