"""
Rotas de Fluxos de Processos (workflow templates)
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_
from uuid import UUID
from datetime import datetime
import logging

from ..database import get_db
from ..models import Fluxo, FluxoNode, FluxoEdge, FluxoProcesso, Equipe, EquipeMembro
from ..schemas import (
    FluxoCreate,
    FluxoUpdate,
    FluxoSaveCanvas,
    FluxoProcessoCreate,
    FluxoProcessoUpdate,
    FluxoResponse,
    FluxoDetalheResponse,
    FluxoNodeResponse,
    FluxoEdgeResponse,
    FluxoProcessoResponse,
    FluxoComVinculacaoResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────


async def _verificar_membro(db: AsyncSession, equipe_id: UUID, usuario: str):
    """Verifica que a equipe existe e o usuario e membro."""
    eq = await db.execute(
        select(Equipe).where(and_(Equipe.id == equipe_id, Equipe.deletado_em.is_(None)))
    )
    if not eq.scalar_one_or_none():
        raise HTTPException(status_code=404, detail="Equipe nao encontrada")

    membro = await db.execute(
        select(EquipeMembro).where(
            and_(
                EquipeMembro.equipe_id == equipe_id,
                EquipeMembro.usuario == usuario,
                EquipeMembro.deletado_em.is_(None),
            )
        )
    )
    if not membro.scalar_one_or_none():
        raise HTTPException(status_code=403, detail="Voce nao e membro desta equipe")


async def _get_fluxo_com_acesso(db: AsyncSession, fluxo_id: UUID, usuario: str) -> Fluxo:
    """Busca fluxo e verifica acesso (dono, membro da equipe, ou mesmo orgao)."""
    result = await db.execute(
        select(Fluxo).where(and_(Fluxo.id == fluxo_id, Fluxo.deletado_em.is_(None)))
    )
    fluxo = result.scalar_one_or_none()
    if not fluxo:
        raise HTTPException(status_code=404, detail="Fluxo nao encontrado")

    # Personal flow: only owner
    if not fluxo.equipe_id and not fluxo.orgao:
        if fluxo.usuario != usuario:
            raise HTTPException(status_code=403, detail="Voce nao tem acesso a este fluxo")
        return fluxo

    # Team flow: check membership
    if fluxo.equipe_id:
        await _verificar_membro(db, fluxo.equipe_id, usuario)
        return fluxo

    # Org flow: accessible to all (org-level access control is out of scope for MVP)
    return fluxo


def _fluxo_to_response(fluxo: Fluxo) -> FluxoResponse:
    """Converts Fluxo model to list-item response with counts."""
    active_nodes = [n for n in fluxo.nodes if n.deletado_em is None]
    active_edges = [e for e in fluxo.edges if e.deletado_em is None]
    active_processos = [p for p in fluxo.processos_vinculados if p.deletado_em is None]
    return FluxoResponse(
        id=fluxo.id,
        nome=fluxo.nome,
        descricao=fluxo.descricao,
        usuario=fluxo.usuario,
        equipe_id=fluxo.equipe_id,
        orgao=fluxo.orgao,
        versao=fluxo.versao,
        status=fluxo.status,
        viewport=fluxo.viewport,
        node_count=len(active_nodes),
        edge_count=len(active_edges),
        processo_count=len(active_processos),
        criado_em=fluxo.criado_em,
        atualizado_em=fluxo.atualizado_em,
    )


def _fluxo_to_detalhe(fluxo: Fluxo) -> FluxoDetalheResponse:
    """Converts Fluxo model to detail response with nodes + edges."""
    active_nodes = [n for n in fluxo.nodes if n.deletado_em is None]
    active_edges = [e for e in fluxo.edges if e.deletado_em is None]
    return FluxoDetalheResponse(
        id=fluxo.id,
        nome=fluxo.nome,
        descricao=fluxo.descricao,
        usuario=fluxo.usuario,
        equipe_id=fluxo.equipe_id,
        orgao=fluxo.orgao,
        versao=fluxo.versao,
        status=fluxo.status,
        viewport=fluxo.viewport,
        nodes=[FluxoNodeResponse.model_validate(n) for n in active_nodes],
        edges=[FluxoEdgeResponse.model_validate(e) for e in active_edges],
        criado_em=fluxo.criado_em,
        atualizado_em=fluxo.atualizado_em,
    )


# ── CRUD Fluxos ───────────────────────────────────────────────


@router.post(
    "",
    response_model=dict,
    status_code=201,
    summary="Criar fluxo",
)
async def criar_fluxo(
    dados: FluxoCreate,
    usuario: str = Query(..., description="Usuario criador"),
    db: AsyncSession = Depends(get_db),
):
    try:
        if dados.equipe_id:
            await _verificar_membro(db, dados.equipe_id, usuario)

        fluxo = Fluxo(
            nome=dados.nome,
            descricao=dados.descricao,
            usuario=usuario,
            equipe_id=dados.equipe_id,
            orgao=dados.orgao,
        )
        db.add(fluxo)
        await db.flush()
        await db.refresh(fluxo)

        return {"status": "success", "data": _fluxo_to_response(fluxo)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao criar fluxo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "",
    response_model=dict,
    summary="Listar fluxos por escopo",
)
async def listar_fluxos(
    usuario: str = Query(..., description="Usuario logado"),
    equipe_id: UUID | None = Query(None, description="Filtrar por equipe"),
    orgao: str | None = Query(None, description="Filtrar por orgao"),
    status: str | None = Query(None, description="Filtrar por status"),
    db: AsyncSession = Depends(get_db),
):
    try:
        conditions = [Fluxo.deletado_em.is_(None)]

        if equipe_id:
            await _verificar_membro(db, equipe_id, usuario)
            conditions.append(Fluxo.equipe_id == equipe_id)
        elif orgao:
            conditions.append(Fluxo.orgao == orgao)
        else:
            # Personal flows
            conditions.append(Fluxo.usuario == usuario)
            conditions.append(Fluxo.equipe_id.is_(None))
            conditions.append(Fluxo.orgao.is_(None))

        if status:
            conditions.append(Fluxo.status == status)

        query = select(Fluxo).where(and_(*conditions)).order_by(Fluxo.atualizado_em.desc())
        result = await db.execute(query)
        fluxos = result.scalars().all()

        return {
            "status": "success",
            "data": [_fluxo_to_response(f) for f in fluxos],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao listar fluxos: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/by-processo",
    response_model=dict,
    summary="Busca fluxos vinculados a um processo (reverse lookup)",
)
async def buscar_fluxos_por_processo(
    numero_processo: str = Query(..., description="Numero do processo (sem formatacao)"),
    usuario: str = Query(..., description="Usuario logado"),
    db: AsyncSession = Depends(get_db),
):
    """Returns all flows that a given process is linked to, with full flow detail."""
    try:
        result = await db.execute(
            select(FluxoProcesso).where(
                and_(
                    FluxoProcesso.numero_processo == numero_processo,
                    FluxoProcesso.deletado_em.is_(None),
                )
            )
        )
        vinculacoes = result.scalars().all()

        items: list[dict] = []
        for fp in vinculacoes:
            try:
                fluxo = await _get_fluxo_com_acesso(db, fp.fluxo_id, usuario)
            except HTTPException:
                continue  # skip flows the user cannot access

            items.append(
                FluxoComVinculacaoResponse(
                    fluxo=_fluxo_to_detalhe(fluxo),
                    vinculacao=FluxoProcessoResponse.model_validate(fp),
                ).model_dump(mode="json")
            )

        return {"status": "success", "data": items}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar fluxos por processo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{fluxo_id}",
    response_model=dict,
    summary="Detalhe do fluxo com nodes e edges",
)
async def detalhe_fluxo(
    fluxo_id: UUID,
    usuario: str = Query(..., description="Usuario logado"),
    db: AsyncSession = Depends(get_db),
):
    try:
        fluxo = await _get_fluxo_com_acesso(db, fluxo_id, usuario)
        return {"status": "success", "data": _fluxo_to_detalhe(fluxo)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao buscar detalhe do fluxo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.patch(
    "/{fluxo_id}",
    response_model=dict,
    summary="Atualizar metadados do fluxo",
)
async def atualizar_fluxo(
    fluxo_id: UUID,
    dados: FluxoUpdate,
    usuario: str = Query(..., description="Usuario logado"),
    db: AsyncSession = Depends(get_db),
):
    try:
        fluxo = await _get_fluxo_com_acesso(db, fluxo_id, usuario)

        if dados.nome is not None:
            fluxo.nome = dados.nome
        if dados.descricao is not None:
            fluxo.descricao = dados.descricao
        if dados.status is not None:
            fluxo.status = dados.status

        fluxo.atualizado_em = datetime.utcnow()
        await db.flush()
        await db.refresh(fluxo)

        return {"status": "success", "data": _fluxo_to_response(fluxo)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao atualizar fluxo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{fluxo_id}",
    response_model=dict,
    summary="Excluir fluxo (soft-delete)",
)
async def deletar_fluxo(
    fluxo_id: UUID,
    usuario: str = Query(..., description="Usuario logado"),
    db: AsyncSession = Depends(get_db),
):
    try:
        fluxo = await _get_fluxo_com_acesso(db, fluxo_id, usuario)
        fluxo.soft_delete()

        # Cascade soft-delete
        for node in fluxo.nodes:
            if node.deletado_em is None:
                node.soft_delete()
        for edge in fluxo.edges:
            if edge.deletado_em is None:
                edge.soft_delete()
        for proc in fluxo.processos_vinculados:
            if proc.deletado_em is None:
                proc.soft_delete()

        await db.commit()
        return {"status": "success", "message": "Fluxo excluido com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar fluxo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ── Canvas Save (Full-Replace) ───────────────────────────────


@router.put(
    "/{fluxo_id}/canvas",
    response_model=dict,
    summary="Salvar canvas (full-replace nodes + edges)",
)
async def salvar_canvas(
    fluxo_id: UUID,
    dados: FluxoSaveCanvas,
    usuario: str = Query(..., description="Usuario logado"),
    db: AsyncSession = Depends(get_db),
):
    try:
        fluxo = await _get_fluxo_com_acesso(db, fluxo_id, usuario)

        # Optimistic concurrency check
        if dados.versao != fluxo.versao:
            raise HTTPException(
                status_code=409,
                detail=f"Conflito de versao: esperado {fluxo.versao}, recebido {dados.versao}. "
                "Recarregue o fluxo e tente novamente.",
            )

        # Soft-delete all existing nodes and edges
        for node in fluxo.nodes:
            if node.deletado_em is None:
                node.soft_delete()
        for edge in fluxo.edges:
            if edge.deletado_em is None:
                edge.soft_delete()

        # Insert new nodes
        for nd in dados.nodes:
            new_node = FluxoNode(
                fluxo_id=fluxo.id,
                node_id=nd.node_id,
                tipo=nd.tipo,
                nome=nd.nome,
                descricao=nd.descricao,
                sei_task_key=nd.sei_task_key,
                responsavel=nd.responsavel,
                duracao_estimada_horas=nd.duracao_estimada_horas,
                prioridade=nd.prioridade,
                documentos_necessarios=nd.documentos_necessarios,
                checklist=nd.checklist,
                regras_prazo=nd.regras_prazo,
                metadata_extra=nd.metadata_extra,
                posicao_x=nd.posicao_x,
                posicao_y=nd.posicao_y,
                largura=nd.largura,
                altura=nd.altura,
            )
            db.add(new_node)

        # Insert new edges
        for ed in dados.edges:
            new_edge = FluxoEdge(
                fluxo_id=fluxo.id,
                edge_id=ed.edge_id,
                source_node_id=ed.source_node_id,
                target_node_id=ed.target_node_id,
                tipo=ed.tipo,
                label=ed.label,
                condicao=ed.condicao,
                ordem=ed.ordem,
                animated=ed.animated,
            )
            db.add(new_edge)

        # Update fluxo metadata
        fluxo.viewport = dados.viewport
        fluxo.versao = fluxo.versao + 1
        fluxo.atualizado_em = datetime.utcnow()

        await db.flush()
        await db.refresh(fluxo)

        return {"status": "success", "data": _fluxo_to_detalhe(fluxo)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao salvar canvas: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


# ── Processos Vinculados ─────────────────────────────────────


@router.post(
    "/{fluxo_id}/processos",
    response_model=dict,
    status_code=201,
    summary="Vincular processo ao fluxo",
)
async def vincular_processo(
    fluxo_id: UUID,
    dados: FluxoProcessoCreate,
    usuario: str = Query(..., description="Usuario que vincula"),
    db: AsyncSession = Depends(get_db),
):
    try:
        fluxo = await _get_fluxo_com_acesso(db, fluxo_id, usuario)

        # Check if already assigned
        existing = await db.execute(
            select(FluxoProcesso).where(
                and_(
                    FluxoProcesso.fluxo_id == fluxo.id,
                    FluxoProcesso.numero_processo == dados.numero_processo,
                    FluxoProcesso.deletado_em.is_(None),
                )
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(status_code=409, detail="Processo ja vinculado a este fluxo")

        fp = FluxoProcesso(
            fluxo_id=fluxo.id,
            numero_processo=dados.numero_processo,
            numero_processo_formatado=dados.numero_processo_formatado,
            node_atual_id=dados.node_atual_id,
            atribuido_por=usuario,
            notas=dados.notas,
            iniciado_em=datetime.utcnow() if dados.node_atual_id else None,
            historico=[
                {
                    "node_id": dados.node_atual_id,
                    "entrada_em": datetime.utcnow().isoformat(),
                    "saida_em": None,
                    "usuario": usuario,
                }
            ]
            if dados.node_atual_id
            else [],
        )
        db.add(fp)
        await db.flush()
        await db.refresh(fp)

        return {"status": "success", "data": FluxoProcessoResponse.model_validate(fp)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao vincular processo: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{fluxo_id}/processos",
    response_model=dict,
    summary="Listar processos vinculados",
)
async def listar_processos_vinculados(
    fluxo_id: UUID,
    usuario: str = Query(..., description="Usuario logado"),
    db: AsyncSession = Depends(get_db),
):
    try:
        fluxo = await _get_fluxo_com_acesso(db, fluxo_id, usuario)

        active = [p for p in fluxo.processos_vinculados if p.deletado_em is None]
        return {
            "status": "success",
            "data": [FluxoProcessoResponse.model_validate(p) for p in active],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao listar processos vinculados: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.patch(
    "/{fluxo_id}/processos/{processo_id}",
    response_model=dict,
    summary="Atualizar vinculacao (avancar etapa, mudar status)",
)
async def atualizar_processo_vinculado(
    fluxo_id: UUID,
    processo_id: UUID,
    dados: FluxoProcessoUpdate,
    usuario: str = Query(..., description="Usuario logado"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _get_fluxo_com_acesso(db, fluxo_id, usuario)

        result = await db.execute(
            select(FluxoProcesso).where(
                and_(
                    FluxoProcesso.id == processo_id,
                    FluxoProcesso.fluxo_id == fluxo_id,
                    FluxoProcesso.deletado_em.is_(None),
                )
            )
        )
        fp = result.scalar_one_or_none()
        if not fp:
            raise HTTPException(status_code=404, detail="Vinculacao nao encontrada")

        now = datetime.utcnow()

        # Advance step
        if dados.node_atual_id is not None and dados.node_atual_id != fp.node_atual_id:
            historico = list(fp.historico or [])

            # Close previous step
            if historico and historico[-1].get("saida_em") is None:
                historico[-1]["saida_em"] = now.isoformat()

            # Open new step
            historico.append(
                {
                    "node_id": dados.node_atual_id,
                    "entrada_em": now.isoformat(),
                    "saida_em": None,
                    "usuario": usuario,
                }
            )
            fp.historico = historico
            fp.node_atual_id = dados.node_atual_id

            if not fp.iniciado_em:
                fp.iniciado_em = now

        if dados.status is not None:
            fp.status = dados.status
            if dados.status == "concluido":
                fp.concluido_em = now
                # Close last step
                historico = list(fp.historico or [])
                if historico and historico[-1].get("saida_em") is None:
                    historico[-1]["saida_em"] = now.isoformat()
                    fp.historico = historico

        if dados.notas is not None:
            fp.notas = dados.notas

        fp.atualizado_em = now
        await db.flush()
        await db.refresh(fp)

        return {"status": "success", "data": FluxoProcessoResponse.model_validate(fp)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao atualizar processo vinculado: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{fluxo_id}/processos/{processo_id}",
    response_model=dict,
    summary="Remover vinculacao do processo",
)
async def remover_processo_vinculado(
    fluxo_id: UUID,
    processo_id: UUID,
    usuario: str = Query(..., description="Usuario logado"),
    db: AsyncSession = Depends(get_db),
):
    try:
        await _get_fluxo_com_acesso(db, fluxo_id, usuario)

        result = await db.execute(
            select(FluxoProcesso).where(
                and_(
                    FluxoProcesso.id == processo_id,
                    FluxoProcesso.fluxo_id == fluxo_id,
                    FluxoProcesso.deletado_em.is_(None),
                )
            )
        )
        fp = result.scalar_one_or_none()
        if not fp:
            raise HTTPException(status_code=404, detail="Vinculacao nao encontrada")

        fp.soft_delete()
        await db.commit()
        return {"status": "success", "message": "Vinculacao removida com sucesso"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao remover processo vinculado: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
