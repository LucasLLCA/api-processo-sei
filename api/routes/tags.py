"""
Rotas para gerenciamento de grupos de processos.

Grupos podem ser pessoais (equipe_id IS NULL) ou de equipe (equipe_id set).
"""
import re
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from datetime import datetime
from uuid import UUID
from typing import Optional
import logging

from ..database import get_db
from ..models import Tag, ProcessoSalvo, Equipe, EquipeMembro
from ..schemas import (
    TagCreate,
    TagUpdate,
    TagResponse,
    ProcessoSalvoCreate,
    ProcessoSalvoResponse,
    TagComProcessosResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


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
    "",
    response_model=dict,
    status_code=201,
    summary="Criar grupo de processos",
)
async def criar_tag(
    dados: TagCreate,
    usuario: str = Query(..., description="Usuario proprietario"),
    equipe_id: Optional[UUID] = Query(None, description="ID da equipe (omitir para grupo pessoal)"),
    db: AsyncSession = Depends(get_db),
):
    try:
        if equipe_id:
            await _verificar_membro(db, equipe_id, usuario)

        tag = Tag(
            nome=dados.nome,
            cor=dados.cor,
            usuario=usuario,
            equipe_id=equipe_id,
        )
        db.add(tag)
        await db.commit()
        await db.refresh(tag)

        scope = f"equipe={equipe_id}" if equipe_id else "pessoal"
        logger.info(f"Grupo criado: {scope}, nome={dados.nome}, usuario={usuario}")

        return {
            "status": "success",
            "data": TagResponse(
                id=tag.id,
                nome=tag.nome,
                usuario=tag.usuario,
                cor=tag.cor,
                criado_em=tag.criado_em,
                atualizado_em=tag.atualizado_em,
                total_processos=0,
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao criar grupo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "",
    response_model=dict,
    summary="Listar grupos de processos",
)
async def listar_tags(
    usuario: str = Query(..., description="Usuario"),
    equipe_id: Optional[UUID] = Query(None, description="ID da equipe (omitir para grupos pessoais)"),
    db: AsyncSession = Depends(get_db),
):
    try:
        if equipe_id:
            await _verificar_membro(db, equipe_id, usuario)
            query = (
                select(Tag)
                .where(and_(Tag.equipe_id == equipe_id, Tag.deletado_em.is_(None)))
                .order_by(Tag.criado_em.desc())
            )
        else:
            query = (
                select(Tag)
                .where(and_(
                    Tag.usuario == usuario,
                    Tag.equipe_id.is_(None),
                    Tag.deletado_em.is_(None),
                ))
                .order_by(Tag.criado_em.desc())
            )

        result = await db.execute(query)
        tags = result.scalars().all()

        tags_response = []
        for tag in tags:
            count_q = select(func.count()).select_from(ProcessoSalvo).where(
                and_(ProcessoSalvo.tag_id == tag.id, ProcessoSalvo.deletado_em.is_(None))
            )
            count_result = await db.execute(count_q)
            total = count_result.scalar()

            tags_response.append(TagResponse(
                id=tag.id,
                nome=tag.nome,
                usuario=tag.usuario,
                cor=tag.cor,
                criado_em=tag.criado_em,
                atualizado_em=tag.atualizado_em,
                total_processos=total,
            ))

        return {"status": "success", "data": tags_response}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao listar grupos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/processo-salvo",
    response_model=dict,
    summary="Verificar se processo esta salvo pelo usuario",
)
async def verificar_processo_salvo(
    usuario: str = Query(..., description="Usuario"),
    numero_processo: str = Query(..., description="Numero do processo"),
    db: AsyncSession = Depends(get_db),
):
    try:
        numero_limpo = re.sub(r'\D', '', numero_processo)
        query = (
            select(Tag.id, Tag.nome, Tag.cor)
            .join(ProcessoSalvo, ProcessoSalvo.tag_id == Tag.id)
            .where(and_(
                Tag.usuario == usuario,
                Tag.equipe_id.is_(None),
                Tag.deletado_em.is_(None),
                ProcessoSalvo.numero_processo == numero_limpo,
                ProcessoSalvo.deletado_em.is_(None),
            ))
        )
        result = await db.execute(query)
        rows = result.all()

        tags_info = [
            {"tag_id": str(row.id), "tag_nome": row.nome, "tag_cor": row.cor}
            for row in rows
        ]

        return {
            "status": "success",
            "data": {
                "salvo": len(tags_info) > 0,
                "tags": tags_info,
            },
        }
    except Exception as e:
        logger.error(f"Erro ao verificar processo salvo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{tag_id}",
    response_model=dict,
    summary="Detalhes do grupo com processos",
)
async def detalhe_tag(
    tag_id: UUID,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        tag = await _get_tag_com_acesso(db, tag_id, usuario)

        processos_q = select(ProcessoSalvo).where(
            and_(ProcessoSalvo.tag_id == tag_id, ProcessoSalvo.deletado_em.is_(None))
        ).order_by(ProcessoSalvo.criado_em.desc())
        result = await db.execute(processos_q)
        processos = result.scalars().all()

        return {
            "status": "success",
            "data": TagComProcessosResponse(
                id=tag.id,
                nome=tag.nome,
                usuario=tag.usuario,
                cor=tag.cor,
                criado_em=tag.criado_em,
                atualizado_em=tag.atualizado_em,
                processos=[ProcessoSalvoResponse.model_validate(p) for p in processos],
            ),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar grupo: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch(
    "/{tag_id}",
    response_model=dict,
    summary="Atualizar grupo",
)
async def atualizar_tag(
    tag_id: UUID,
    dados: TagUpdate,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        tag = await _get_tag_com_acesso(db, tag_id, usuario)

        if dados.nome is not None:
            tag.nome = dados.nome
        if dados.cor is not None:
            tag.cor = dados.cor
        tag.atualizado_em = datetime.utcnow()

        await db.commit()
        await db.refresh(tag)

        return {"status": "success", "data": TagResponse.model_validate(tag)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao atualizar grupo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{tag_id}",
    response_model=dict,
    summary="Excluir grupo",
)
async def deletar_tag(
    tag_id: UUID,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        tag = await _get_tag_com_acesso(db, tag_id, usuario)
        tag.soft_delete()

        # Soft-delete dos processos associados
        processos_q = select(ProcessoSalvo).where(
            and_(ProcessoSalvo.tag_id == tag_id, ProcessoSalvo.deletado_em.is_(None))
        )
        result = await db.execute(processos_q)
        for p in result.scalars().all():
            p.soft_delete()

        await db.commit()

        return {"status": "success", "message": "Grupo excluido com sucesso"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar grupo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/{tag_id}/processos",
    response_model=dict,
    status_code=201,
    summary="Salvar processo no grupo",
)
async def salvar_processo(
    tag_id: UUID,
    dados: ProcessoSalvoCreate,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _get_tag_com_acesso(db, tag_id, usuario)

        # Verificar duplicata
        existente_q = await db.execute(
            select(ProcessoSalvo).where(and_(
                ProcessoSalvo.tag_id == tag_id,
                ProcessoSalvo.numero_processo == dados.numero_processo,
                ProcessoSalvo.deletado_em.is_(None),
            ))
        )
        if existente_q.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Processo ja salvo neste grupo")

        processo = ProcessoSalvo(
            tag_id=tag_id,
            numero_processo=dados.numero_processo,
            numero_processo_formatado=dados.numero_processo_formatado,
            nota=dados.nota,
        )
        db.add(processo)
        await db.commit()
        await db.refresh(processo)

        return {
            "status": "success",
            "data": ProcessoSalvoResponse.model_validate(processo),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao salvar processo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{tag_id}/processos/{processo_id}",
    response_model=dict,
    summary="Remover processo do grupo",
)
async def remover_processo(
    tag_id: UUID,
    processo_id: UUID,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _get_tag_com_acesso(db, tag_id, usuario)

        result = await db.execute(
            select(ProcessoSalvo).where(and_(
                ProcessoSalvo.id == processo_id,
                ProcessoSalvo.tag_id == tag_id,
                ProcessoSalvo.deletado_em.is_(None),
            ))
        )
        processo = result.scalar_one_or_none()
        if not processo:
            raise HTTPException(status_code=404, detail="Processo nao encontrado no grupo")

        processo.soft_delete()
        await db.commit()

        return {"status": "success", "message": "Processo removido do grupo"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao remover processo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# --- Helpers ---

async def _get_tag_com_acesso(db: AsyncSession, tag_id: UUID, usuario: str) -> Tag:
    """Busca grupo e verifica acesso (dono ou membro da equipe)."""
    result = await db.execute(
        select(Tag).where(and_(Tag.id == tag_id, Tag.deletado_em.is_(None)))
    )
    tag = result.scalar_one_or_none()
    if not tag:
        raise HTTPException(status_code=404, detail="Grupo nao encontrado")

    if tag.equipe_id:
        # Team grupo: verify membership
        await _verificar_membro(db, tag.equipe_id, usuario)
    elif tag.usuario != usuario:
        raise HTTPException(status_code=403, detail="Voce nao e o proprietario deste grupo")

    return tag
