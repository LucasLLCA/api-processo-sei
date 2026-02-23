"""
Rotas para gerenciamento de tags e processos salvos
"""
import re
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from datetime import datetime
from uuid import UUID
import logging

from ..database import get_db
from ..models import Tag, ProcessoSalvo
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


@router.post(
    "",
    response_model=dict,
    status_code=201,
    summary="Criar tag",
)
async def criar_tag(
    dados: TagCreate,
    usuario: str = Query(..., description="Usuário proprietário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        tag = Tag(
            nome=dados.nome,
            cor=dados.cor,
            usuario=usuario,
        )
        db.add(tag)
        await db.commit()
        await db.refresh(tag)

        logger.info(f"Tag criada: nome={dados.nome}, usuario={usuario}")

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
    except Exception as e:
        logger.error(f"Erro ao criar tag: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "",
    response_model=dict,
    summary="Listar tags do usuário",
)
async def listar_tags(
    usuario: str = Query(..., description="Usuário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        query = (
            select(Tag)
            .where(and_(Tag.usuario == usuario, Tag.deletado_em.is_(None)))
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

    except Exception as e:
        logger.error(f"Erro ao listar tags: {e}")
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
    summary="Detalhes da tag com processos",
)
async def detalhe_tag(
    tag_id: UUID,
    usuario: str = Query(..., description="Usuário proprietário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        tag = await _get_tag_do_usuario(db, tag_id, usuario)

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
        logger.error(f"Erro ao buscar tag: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.patch(
    "/{tag_id}",
    response_model=dict,
    summary="Atualizar tag",
)
async def atualizar_tag(
    tag_id: UUID,
    dados: TagUpdate,
    usuario: str = Query(..., description="Usuário proprietário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        tag = await _get_tag_do_usuario(db, tag_id, usuario)

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
        logger.error(f"Erro ao atualizar tag: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{tag_id}",
    response_model=dict,
    summary="Excluir tag",
)
async def deletar_tag(
    tag_id: UUID,
    usuario: str = Query(..., description="Usuário proprietário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        tag = await _get_tag_do_usuario(db, tag_id, usuario)
        tag.soft_delete()

        # Soft-delete dos processos associados
        processos_q = select(ProcessoSalvo).where(
            and_(ProcessoSalvo.tag_id == tag_id, ProcessoSalvo.deletado_em.is_(None))
        )
        result = await db.execute(processos_q)
        for p in result.scalars().all():
            p.soft_delete()

        await db.commit()

        return {"status": "success", "message": "Tag excluída com sucesso"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar tag: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/{tag_id}/processos",
    response_model=dict,
    status_code=201,
    summary="Salvar processo na tag",
)
async def salvar_processo(
    tag_id: UUID,
    dados: ProcessoSalvoCreate,
    usuario: str = Query(..., description="Usuário proprietário da tag"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _get_tag_do_usuario(db, tag_id, usuario)

        # Verificar duplicata
        existente_q = await db.execute(
            select(ProcessoSalvo).where(and_(
                ProcessoSalvo.tag_id == tag_id,
                ProcessoSalvo.numero_processo == dados.numero_processo,
                ProcessoSalvo.deletado_em.is_(None),
            ))
        )
        if existente_q.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Processo já salvo nesta tag")

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
    summary="Remover processo da tag",
)
async def remover_processo(
    tag_id: UUID,
    processo_id: UUID,
    usuario: str = Query(..., description="Usuário proprietário da tag"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _get_tag_do_usuario(db, tag_id, usuario)

        result = await db.execute(
            select(ProcessoSalvo).where(and_(
                ProcessoSalvo.id == processo_id,
                ProcessoSalvo.tag_id == tag_id,
                ProcessoSalvo.deletado_em.is_(None),
            ))
        )
        processo = result.scalar_one_or_none()
        if not processo:
            raise HTTPException(status_code=404, detail="Processo não encontrado na tag")

        processo.soft_delete()
        await db.commit()

        return {"status": "success", "message": "Processo removido da tag"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao remover processo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# --- Helpers ---

async def _get_tag_do_usuario(db: AsyncSession, tag_id: UUID, usuario: str) -> Tag:
    result = await db.execute(
        select(Tag).where(and_(Tag.id == tag_id, Tag.deletado_em.is_(None)))
    )
    tag = result.scalar_one_or_none()
    if not tag:
        raise HTTPException(status_code=404, detail="Tag não encontrada")
    if tag.usuario != usuario:
        raise HTTPException(status_code=403, detail="Você não é o proprietário desta tag")
    return tag
