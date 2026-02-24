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
from ..models import Equipe, EquipeMembro, Compartilhamento, Tag, ProcessoSalvo, TeamTag, ProcessoTeamTag
from ..schemas import (
    EquipeCreate,
    EquipeUpdate,
    MembroAdd,
    MembroResponse,
    EquipeResponse,
    EquipeDetalheResponse,
    ProcessoSalvoResponse,
    TeamTagResponse,
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


@router.get(
    "/{equipe_id}/kanban",
    response_model=dict,
    summary="Kanban board da equipe",
)
async def kanban_board(
    equipe_id: UUID,
    usuario: str = Query(..., description="Usuario"),
    db: AsyncSession = Depends(get_db),
):
    try:
        equipe = await _get_equipe_como_membro(db, equipe_id, usuario)

        # Buscar membros para EquipeDetalheResponse
        membros_q = await db.execute(
            select(EquipeMembro).where(and_(
                EquipeMembro.equipe_id == equipe_id,
                EquipeMembro.deletado_em.is_(None),
            ))
        )
        membros = membros_q.scalars().all()

        equipe_data = EquipeDetalheResponse(
            id=equipe.id,
            nome=equipe.nome,
            descricao=equipe.descricao,
            proprietario_usuario=equipe.proprietario_usuario,
            criado_em=equipe.criado_em,
            atualizado_em=equipe.atualizado_em,
            membros=[MembroResponse.model_validate(m) for m in membros],
        )

        # Buscar compartilhamentos para esta equipe
        comp_q = await db.execute(
            select(Compartilhamento).where(and_(
                Compartilhamento.equipe_destino_id == equipe_id,
                Compartilhamento.deletado_em.is_(None),
            )).order_by(Compartilhamento.criado_em.asc())
        )
        compartilhamentos = comp_q.scalars().all()

        colunas = []
        for c in compartilhamentos:
            # Buscar tag (grupo de processos)
            tag_q = await db.execute(
                select(Tag).where(and_(Tag.id == c.tag_id, Tag.deletado_em.is_(None)))
            )
            tag = tag_q.scalar_one_or_none()
            if not tag:
                continue

            # Buscar processos da tag
            proc_q = await db.execute(
                select(ProcessoSalvo).where(and_(
                    ProcessoSalvo.tag_id == tag.id,
                    ProcessoSalvo.deletado_em.is_(None),
                )).order_by(ProcessoSalvo.criado_em.desc())
            )
            processos = proc_q.scalars().all()

            # Para cada processo, buscar team_tags desta equipe
            processos_com_tags = []
            for p in processos:
                ptag_q = await db.execute(
                    select(TeamTag)
                    .join(ProcessoTeamTag, ProcessoTeamTag.team_tag_id == TeamTag.id)
                    .where(and_(
                        ProcessoTeamTag.numero_processo == p.numero_processo,
                        ProcessoTeamTag.deletado_em.is_(None),
                        TeamTag.equipe_id == equipe_id,
                        TeamTag.deletado_em.is_(None),
                    ))
                )
                team_tags = [TeamTagResponse.model_validate(t) for t in ptag_q.scalars().all()]

                proc_data = ProcessoSalvoResponse.model_validate(p).model_dump()
                proc_data["team_tags"] = [t.model_dump() for t in team_tags]
                processos_com_tags.append(proc_data)

            colunas.append({
                "compartilhamento_id": str(c.id),
                "tag_id": str(tag.id),
                "tag_nome": tag.nome,
                "tag_cor": tag.cor,
                "compartilhado_por": c.compartilhado_por,
                "processos": processos_com_tags,
            })

        # Buscar paleta de team tags
        all_tags_q = await db.execute(
            select(TeamTag).where(and_(
                TeamTag.equipe_id == equipe_id,
                TeamTag.deletado_em.is_(None),
            )).order_by(TeamTag.nome.asc())
        )
        all_team_tags = [TeamTagResponse.model_validate(t) for t in all_tags_q.scalars().all()]

        return {
            "status": "success",
            "data": {
                "equipe": equipe_data.model_dump(),
                "colunas": colunas,
                "team_tags": [t.model_dump() for t in all_team_tags],
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar kanban: {e}")
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
