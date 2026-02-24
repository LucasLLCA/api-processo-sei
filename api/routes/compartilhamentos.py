"""
Rotas para gerenciamento de compartilhamentos
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_
from uuid import UUID
import logging

from ..database import get_db
from ..models import Tag, ProcessoSalvo, Compartilhamento, Equipe, EquipeMembro, TeamTag, ProcessoTeamTag
from ..schemas import (
    CompartilhamentoCreate,
    CompartilhamentoResponse,
    CompartilhadoComMigoItem,
    ProcessoSalvoResponse,
    TeamTagResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post(
    "",
    response_model=dict,
    status_code=201,
    summary="Compartilhar tag",
)
async def compartilhar_tag(
    dados: CompartilhamentoCreate,
    usuario: str = Query(..., description="Usuário que compartilha"),
    db: AsyncSession = Depends(get_db),
):
    try:
        # Verificar que o usuário é dono da tag
        tag_q = await db.execute(
            select(Tag).where(and_(
                Tag.id == dados.tag_id,
                Tag.usuario == usuario,
                Tag.deletado_em.is_(None),
            ))
        )
        if not tag_q.scalar_one_or_none():
            raise HTTPException(status_code=403, detail="Você não é o proprietário desta tag")

        # Verificar duplicata: mesmo tag_id + mesmo destino
        dup_conditions = [
            Compartilhamento.tag_id == dados.tag_id,
            Compartilhamento.deletado_em.is_(None),
        ]
        if dados.equipe_destino_id:
            dup_conditions.append(Compartilhamento.equipe_destino_id == dados.equipe_destino_id)
        elif dados.usuario_destino:
            dup_conditions.append(Compartilhamento.usuario_destino == dados.usuario_destino)

        dup_q = await db.execute(select(Compartilhamento).where(and_(*dup_conditions)))
        if dup_q.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Este grupo já está compartilhado com este destino")

        compartilhamento = Compartilhamento(
            tag_id=dados.tag_id,
            compartilhado_por=usuario,
            equipe_destino_id=dados.equipe_destino_id,
            usuario_destino=dados.usuario_destino,
        )
        db.add(compartilhamento)
        await db.commit()
        await db.refresh(compartilhamento)

        logger.info(f"Tag compartilhada: tag_id={dados.tag_id}, por={usuario}")

        return {
            "status": "success",
            "data": CompartilhamentoResponse.model_validate(compartilhamento),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao compartilhar: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/recebidos",
    response_model=dict,
    summary="Compartilhados comigo",
)
async def recebidos(
    usuario: str = Query(..., description="Usuário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        # Equipes do usuário
        equipes_q = select(EquipeMembro.equipe_id).where(and_(
            EquipeMembro.usuario == usuario,
            EquipeMembro.deletado_em.is_(None),
        ))
        equipes_result = await db.execute(equipes_q)
        equipe_ids = [row[0] for row in equipes_result.all()]

        # Compartilhamentos diretos ou via equipe
        conditions = [
            and_(
                Compartilhamento.usuario_destino == usuario,
                Compartilhamento.deletado_em.is_(None),
            )
        ]
        if equipe_ids:
            conditions.append(
                and_(
                    Compartilhamento.equipe_destino_id.in_(equipe_ids),
                    Compartilhamento.deletado_em.is_(None),
                )
            )

        compartilhamentos_q = (
            select(Compartilhamento)
            .where(or_(*conditions))
            .order_by(Compartilhamento.criado_em.desc())
        )
        result = await db.execute(compartilhamentos_q)
        compartilhamentos = result.scalars().all()

        items = []
        for c in compartilhamentos:
            # Buscar tag
            tag_q = await db.execute(
                select(Tag).where(and_(Tag.id == c.tag_id, Tag.deletado_em.is_(None)))
            )
            tag = tag_q.scalar_one_or_none()
            if not tag:
                continue

            # Buscar processos da tag
            processos_q = await db.execute(
                select(ProcessoSalvo).where(
                    and_(ProcessoSalvo.tag_id == tag.id, ProcessoSalvo.deletado_em.is_(None))
                ).order_by(ProcessoSalvo.criado_em.desc())
            )
            processos = processos_q.scalars().all()

            # Nome da equipe (se via equipe)
            equipe_nome = None
            if c.equipe_destino_id:
                eq_q = await db.execute(
                    select(Equipe.nome).where(Equipe.id == c.equipe_destino_id)
                )
                row = eq_q.first()
                equipe_nome = row[0] if row else None

            # Enriquecer processos com team_tags se compartilhado via equipe
            processos_data = []
            for p in processos:
                proc_dict = ProcessoSalvoResponse.model_validate(p).model_dump()
                proc_dict["team_tags"] = []
                if c.equipe_destino_id:
                    ptag_q = await db.execute(
                        select(TeamTag)
                        .join(ProcessoTeamTag, ProcessoTeamTag.team_tag_id == TeamTag.id)
                        .where(and_(
                            ProcessoTeamTag.numero_processo == p.numero_processo,
                            ProcessoTeamTag.deletado_em.is_(None),
                            TeamTag.equipe_id == c.equipe_destino_id,
                            TeamTag.deletado_em.is_(None),
                        ))
                    )
                    proc_dict["team_tags"] = [
                        TeamTagResponse.model_validate(t).model_dump()
                        for t in ptag_q.scalars().all()
                    ]
                processos_data.append(proc_dict)

            items.append({
                "compartilhamento_id": str(c.id),
                "tag_id": str(tag.id),
                "tag_nome": tag.nome,
                "tag_cor": tag.cor,
                "compartilhado_por": c.compartilhado_por,
                "equipe_nome": equipe_nome,
                "equipe_destino_id": str(c.equipe_destino_id) if c.equipe_destino_id else None,
                "criado_em": c.criado_em.isoformat() if c.criado_em else None,
                "processos": processos_data,
            })

        return {"status": "success", "data": items}

    except Exception as e:
        logger.error(f"Erro ao buscar recebidos: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/enviados",
    response_model=dict,
    summary="Compartilhamentos enviados",
)
async def enviados(
    usuario: str = Query(..., description="Usuário"),
    db: AsyncSession = Depends(get_db),
):
    try:
        query = (
            select(Compartilhamento)
            .where(and_(
                Compartilhamento.compartilhado_por == usuario,
                Compartilhamento.deletado_em.is_(None),
            ))
            .order_by(Compartilhamento.criado_em.desc())
        )
        result = await db.execute(query)
        compartilhamentos = result.scalars().all()

        return {
            "status": "success",
            "data": [CompartilhamentoResponse.model_validate(c) for c in compartilhamentos],
        }

    except Exception as e:
        logger.error(f"Erro ao buscar enviados: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{compartilhamento_id}",
    response_model=dict,
    summary="Revogar compartilhamento",
)
async def revogar(
    compartilhamento_id: UUID,
    usuario: str = Query(..., description="Usuário que compartilhou"),
    db: AsyncSession = Depends(get_db),
):
    try:
        result = await db.execute(
            select(Compartilhamento).where(and_(
                Compartilhamento.id == compartilhamento_id,
                Compartilhamento.deletado_em.is_(None),
            ))
        )
        compartilhamento = result.scalar_one_or_none()
        if not compartilhamento:
            raise HTTPException(status_code=404, detail="Compartilhamento não encontrado")
        if compartilhamento.compartilhado_por != usuario:
            raise HTTPException(status_code=403, detail="Apenas quem compartilhou pode revogar")

        compartilhamento.soft_delete()
        await db.commit()

        return {"status": "success", "message": "Compartilhamento revogado"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao revogar compartilhamento: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
