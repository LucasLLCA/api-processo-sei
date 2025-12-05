"""
Rotas para gerenciamento do histórico de pesquisas de processos
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, desc
from datetime import datetime
from typing import Optional
from uuid import UUID
import logging

from ..database import get_db
from ..models import HistoricoPesquisa
from ..schemas import (
    HistoricoPesquisaCreate,
    HistoricoPesquisaUpdate,
    HistoricoPesquisaResponse,
    HistoricoPesquisaList,
    HistoricoPesquisaVerificacao,
    HistoricoPesquisaStats,
    HistoricoPesquisaDeleteResponse,
    HistoricoPesquisaRestoreResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post(
    "",
    response_model=dict,
    status_code=201,
    summary="Salvar pesquisa no histórico",
    description="Registra uma nova pesquisa de processo no histórico"
)
async def criar_historico(
    dados: HistoricoPesquisaCreate,
    db: AsyncSession = Depends(get_db)
):
    """Cria um novo registro de histórico de pesquisa"""
    try:
        novo_historico = HistoricoPesquisa(
            numero_processo=dados.numero_processo,
            numero_processo_formatado=dados.numero_processo_formatado,
            usuario=dados.usuario,
            caixa_contexto=dados.caixa_contexto
        )

        db.add(novo_historico)
        await db.commit()
        await db.refresh(novo_historico)

        logger.info(
            f"Histórico criado: processo={dados.numero_processo}, "
            f"usuario={dados.usuario}"
        )

        return {
            "status": "success",
            "data": HistoricoPesquisaResponse.model_validate(novo_historico)
        }

    except Exception as e:
        logger.error(f"Erro ao criar histórico: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao salvar histórico: {str(e)}"
        )


@router.get(
    "/{usuario}",
    response_model=dict,
    summary="Mostrar histórico de um usuário",
    description="Retorna o histórico de pesquisas de um usuário específico"
)
async def listar_historico_usuario(
    usuario: str,
    limit: int = Query(50, ge=1, le=100, description="Limite de registros"),
    offset: int = Query(0, ge=0, description="Deslocamento para paginação"),
    incluir_deletados: bool = Query(False, description="Incluir registros deletados"),
    db: AsyncSession = Depends(get_db)
):
    """Lista o histórico de pesquisas de um usuário"""
    try:
        # Query base
        base_query = select(HistoricoPesquisa).where(
            HistoricoPesquisa.usuario == usuario
        )

        # Filtrar deletados
        if not incluir_deletados:
            base_query = base_query.where(
                HistoricoPesquisa.deletado_em.is_(None)
            )

        # Contar total
        count_query = select(func.count()).select_from(base_query.subquery())
        total_result = await db.execute(count_query)
        total = total_result.scalar()

        # Buscar registros com paginação
        query = base_query.order_by(
            desc(HistoricoPesquisa.criado_em)
        ).limit(limit).offset(offset)

        result = await db.execute(query)
        pesquisas = result.scalars().all()

        return {
            "status": "success",
            "data": HistoricoPesquisaList(
                usuario=usuario,
                total=total,
                limit=limit,
                offset=offset,
                pesquisas=[
                    HistoricoPesquisaResponse.model_validate(p)
                    for p in pesquisas
                ]
            )
        }

    except Exception as e:
        logger.error(f"Erro ao listar histórico: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao buscar histórico: {str(e)}"
        )


@router.get(
    "/{usuario}/processos/{numero_processo}",
    response_model=dict,
    summary="Verificar se usuário já pesquisou um processo",
    description="Verifica se o usuário já pesquisou um processo específico"
)
async def verificar_pesquisa_processo(
    usuario: str,
    numero_processo: str,
    db: AsyncSession = Depends(get_db)
):
    """Verifica se o usuário já pesquisou determinado processo"""
    try:
        # Buscar pesquisas do processo pelo usuário
        query = select(HistoricoPesquisa).where(
            and_(
                HistoricoPesquisa.usuario == usuario,
                HistoricoPesquisa.numero_processo == numero_processo,
                HistoricoPesquisa.deletado_em.is_(None)
            )
        ).order_by(desc(HistoricoPesquisa.criado_em))

        result = await db.execute(query)
        pesquisas = result.scalars().all()

        if not pesquisas:
            return {
                "status": "success",
                "data": HistoricoPesquisaVerificacao(
                    numero_processo=numero_processo,
                    numero_processo_formatado=None,
                    ja_pesquisado=False,
                    total_pesquisas=0,
                    ultima_pesquisa=None,
                    primeira_pesquisa=None
                )
            }

        ultima = pesquisas[0]
        primeira = pesquisas[-1]

        return {
            "status": "success",
            "data": HistoricoPesquisaVerificacao(
                numero_processo=numero_processo,
                numero_processo_formatado=ultima.numero_processo_formatado,
                ja_pesquisado=True,
                total_pesquisas=len(pesquisas),
                ultima_pesquisa={
                    "id": str(ultima.id),
                    "criado_em": ultima.criado_em.isoformat(),
                    "caixa_contexto": ultima.caixa_contexto
                },
                primeira_pesquisa={
                    "id": str(primeira.id),
                    "criado_em": primeira.criado_em.isoformat(),
                    "caixa_contexto": primeira.caixa_contexto
                }
            )
        }

    except Exception as e:
        logger.error(f"Erro ao verificar pesquisa: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao verificar pesquisa: {str(e)}"
        )


@router.patch(
    "/{id}",
    response_model=dict,
    summary="Atualizar contexto de uma pesquisa",
    description="Atualiza o campo caixa_contexto de uma pesquisa"
)
async def atualizar_historico(
    id: UUID,
    dados: HistoricoPesquisaUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Atualiza o contexto de um registro de histórico"""
    try:
        # Buscar registro
        query = select(HistoricoPesquisa).where(
            HistoricoPesquisa.id == id
        )
        result = await db.execute(query)
        historico = result.scalar_one_or_none()

        if not historico:
            raise HTTPException(
                status_code=404,
                detail="Pesquisa não encontrada"
            )

        # Atualizar
        historico.caixa_contexto = dados.caixa_contexto
        await db.commit()
        await db.refresh(historico)

        logger.info(f"Histórico atualizado: id={id}")

        return {
            "status": "success",
            "data": HistoricoPesquisaResponse.model_validate(historico)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao atualizar histórico: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao atualizar histórico: {str(e)}"
        )


@router.delete(
    "/{usuario}",
    response_model=dict,
    summary="Apagar histórico de um usuário",
    description="Realiza soft delete de todo o histórico de um usuário"
)
async def deletar_historico_usuario(
    usuario: str,
    db: AsyncSession = Depends(get_db)
):
    """Soft delete de todo histórico de um usuário"""
    try:
        # Buscar registros não deletados
        query = select(HistoricoPesquisa).where(
            and_(
                HistoricoPesquisa.usuario == usuario,
                HistoricoPesquisa.deletado_em.is_(None)
            )
        )
        result = await db.execute(query)
        registros = result.scalars().all()

        if not registros:
            return {
                "status": "success",
                "message": "Nenhum registro encontrado para deletar",
                "data": HistoricoPesquisaDeleteResponse(
                    message="Nenhum registro encontrado",
                    usuario=usuario,
                    registros_apagados=0
                )
            }

        # Soft delete
        agora = datetime.utcnow()
        for registro in registros:
            registro.soft_delete()

        await db.commit()

        logger.info(
            f"Histórico deletado: usuario={usuario}, "
            f"registros={len(registros)}"
        )

        return {
            "status": "success",
            "message": "Histórico do usuário apagado com sucesso",
            "data": HistoricoPesquisaDeleteResponse(
                message="Histórico apagado com sucesso",
                usuario=usuario,
                registros_apagados=len(registros),
                deletado_em=agora
            )
        }

    except Exception as e:
        logger.error(f"Erro ao deletar histórico: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao deletar histórico: {str(e)}"
        )


@router.delete(
    "/pesquisa/{id}",
    response_model=dict,
    summary="Apagar uma pesquisa específica",
    description="Realiza soft delete de uma pesquisa específica"
)
async def deletar_pesquisa(
    id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Soft delete de uma pesquisa específica"""
    try:
        # Buscar registro
        query = select(HistoricoPesquisa).where(
            HistoricoPesquisa.id == id
        )
        result = await db.execute(query)
        historico = result.scalar_one_or_none()

        if not historico:
            raise HTTPException(
                status_code=404,
                detail="Pesquisa não encontrada"
            )

        # Soft delete
        historico.soft_delete()
        await db.commit()

        logger.info(f"Pesquisa deletada: id={id}")

        return {
            "status": "success",
            "message": "Pesquisa removida do histórico",
            "data": HistoricoPesquisaDeleteResponse(
                message="Pesquisa removida",
                id=id,
                deletado_em=historico.deletado_em
            )
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar pesquisa: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao deletar pesquisa: {str(e)}"
        )


@router.delete(
    "/pesquisa/{id}/permanente",
    response_model=dict,
    summary="Apagar permanentemente uma pesquisa",
    description="Remove permanentemente uma pesquisa do banco de dados"
)
async def deletar_pesquisa_permanente(
    id: UUID,
    db: AsyncSession = Depends(get_db)
):
    """Delete permanente de uma pesquisa"""
    try:
        # Buscar registro
        query = select(HistoricoPesquisa).where(
            HistoricoPesquisa.id == id
        )
        result = await db.execute(query)
        historico = result.scalar_one_or_none()

        if not historico:
            raise HTTPException(
                status_code=404,
                detail="Pesquisa não encontrada"
            )

        # Delete permanente
        await db.delete(historico)
        await db.commit()

        logger.warning(f"Pesquisa deletada permanentemente: id={id}")

        return {
            "status": "success",
            "message": "Pesquisa removida permanentemente do histórico"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao deletar permanentemente: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao deletar permanentemente: {str(e)}"
        )


@router.post(
    "/{usuario}/restaurar",
    response_model=dict,
    summary="Restaurar histórico deletado",
    description="Restaura todos os registros deletados de um usuário"
)
async def restaurar_historico_usuario(
    usuario: str,
    db: AsyncSession = Depends(get_db)
):
    """Restaura registros deletados de um usuário"""
    try:
        # Buscar registros deletados
        query = select(HistoricoPesquisa).where(
            and_(
                HistoricoPesquisa.usuario == usuario,
                HistoricoPesquisa.deletado_em.isnot(None)
            )
        )
        result = await db.execute(query)
        registros = result.scalars().all()

        if not registros:
            return {
                "status": "success",
                "message": "Nenhum registro deletado encontrado",
                "data": HistoricoPesquisaRestoreResponse(
                    message="Nenhum registro para restaurar",
                    usuario=usuario,
                    registros_restaurados=0
                )
            }

        # Restaurar
        for registro in registros:
            registro.restore()

        await db.commit()

        logger.info(
            f"Histórico restaurado: usuario={usuario}, "
            f"registros={len(registros)}"
        )

        return {
            "status": "success",
            "message": "Histórico do usuário restaurado com sucesso",
            "data": HistoricoPesquisaRestoreResponse(
                message="Histórico restaurado com sucesso",
                usuario=usuario,
                registros_restaurados=len(registros)
            )
        }

    except Exception as e:
        logger.error(f"Erro ao restaurar histórico: {str(e)}")
        await db.rollback()
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao restaurar histórico: {str(e)}"
        )


@router.get(
    "/processos/mais-pesquisados",
    response_model=dict,
    summary="Processos mais pesquisados",
    description="Retorna estatísticas dos processos mais pesquisados"
)
async def processos_mais_pesquisados(
    limit: int = Query(10, ge=1, le=100, description="Limite de resultados"),
    db: AsyncSession = Depends(get_db)
):
    """Retorna os processos mais pesquisados"""
    try:
        # Query para contar pesquisas por processo
        query = (
            select(
                HistoricoPesquisa.numero_processo,
                HistoricoPesquisa.numero_processo_formatado,
                func.count(HistoricoPesquisa.id).label("total_pesquisas"),
                func.count(func.distinct(HistoricoPesquisa.usuario)).label("total_usuarios"),
                func.max(HistoricoPesquisa.criado_em).label("ultima_pesquisa")
            )
            .where(HistoricoPesquisa.deletado_em.is_(None))
            .group_by(
                HistoricoPesquisa.numero_processo,
                HistoricoPesquisa.numero_processo_formatado
            )
            .order_by(desc("total_pesquisas"))
            .limit(limit)
        )

        result = await db.execute(query)
        rows = result.all()

        stats = [
            HistoricoPesquisaStats(
                numero_processo=row.numero_processo,
                numero_processo_formatado=row.numero_processo_formatado,
                total_pesquisas=row.total_pesquisas,
                total_usuarios=row.total_usuarios,
                ultima_pesquisa=row.ultima_pesquisa
            )
            for row in rows
        ]

        return {
            "status": "success",
            "data": stats
        }

    except Exception as e:
        logger.error(f"Erro ao buscar estatísticas: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Erro ao buscar estatísticas: {str(e)}"
        )
