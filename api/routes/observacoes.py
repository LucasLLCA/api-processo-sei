"""
Rotas para gerenciamento de observacoes sobre processos
"""
import re
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, update, func
from sqlalchemy.orm import selectinload
from uuid import UUID
import logging
from datetime import datetime

from ..database import get_db
from ..models import Observacao, EquipeMembro, ObservacaoMencao
from ..schemas import ObservacaoCreate, ObservacaoUpdate, ObservacaoResponse

router = APIRouter()
logger = logging.getLogger(__name__)


def _strip_non_digits(value: str) -> str:
    return re.sub(r'\D', '', value)


def _extrair_mencoes(conteudo: str) -> list[str]:
    """Extrai @usuarios do conteudo da observacao."""
    mencoes = re.findall(r'@([\w.]+(?:@[\w.]+)*)', conteudo)
    vistos: set[str] = set()
    unicos = []
    for m in mencoes:
        if m not in vistos:
            vistos.add(m)
            unicos.append(m)
    return unicos


@router.get(
    "/{numero_processo}/mencoes-nao-lidas",
    response_model=dict,
    summary="Contar mencoes nao lidas para um usuario neste processo",
)
async def mencoes_nao_lidas(
    numero_processo: str,
    usuario: str = Query(..., description="Usuario logado"),
    db: AsyncSession = Depends(get_db),
):
    try:
        numero_limpo = _strip_non_digits(numero_processo)

        count_q = await db.execute(
            select(func.count(ObservacaoMencao.id))
            .join(Observacao, ObservacaoMencao.observacao_id == Observacao.id)
            .where(and_(
                Observacao.numero_processo == numero_limpo,
                Observacao.deletado_em.is_(None),
                ObservacaoMencao.usuario_mencionado == usuario,
                ObservacaoMencao.visto_em.is_(None),
            ))
        )
        count = count_q.scalar_one()

        return {"status": "success", "count": count}
    except Exception as e:
        logger.error(f"Erro ao contar mencoes nao lidas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get(
    "/{numero_processo}",
    response_model=dict,
    summary="Listar observacoes de um processo",
)
async def listar_observacoes(
    numero_processo: str,
    equipe_id: UUID | None = Query(None, description="Filtrar obs de equipe por equipe especifica"),
    usuario: str | None = Query(None, description="Usuario logado (necessario para ver obs pessoais e de equipe)"),
    db: AsyncSession = Depends(get_db),
):
    try:
        numero_limpo = _strip_non_digits(numero_processo)

        conditions = [
            Observacao.numero_processo == numero_limpo,
            Observacao.deletado_em.is_(None),
            Observacao.parent_id.is_(None),  # apenas obs raiz; respostas vem via relationship
        ]

        if usuario:
            if equipe_id is not None:
                membro_q = await db.execute(
                    select(EquipeMembro).where(and_(
                        EquipeMembro.equipe_id == equipe_id,
                        EquipeMembro.usuario == usuario,
                        EquipeMembro.deletado_em.is_(None),
                    ))
                )
                if not membro_q.scalar_one_or_none():
                    raise HTTPException(status_code=403, detail="Voce nao e membro desta equipe")
                equipe_condition = Observacao.equipe_id == equipe_id
            else:
                equipe_condition = Observacao.equipe_id.in_(
                    select(EquipeMembro.equipe_id).where(
                        and_(
                            EquipeMembro.usuario == usuario,
                            EquipeMembro.deletado_em.is_(None),
                        )
                    )
                )

            visibility = or_(
                Observacao.escopo == 'global',
                and_(
                    Observacao.escopo == 'pessoal',
                    or_(
                        Observacao.usuario == usuario,  # é o autor
                        Observacao.id.in_(              # ou foi mencionado
                            select(ObservacaoMencao.observacao_id)
                            .where(ObservacaoMencao.usuario_mencionado == usuario)
                        ),
                    ),
                ),
                and_(
                    Observacao.escopo == 'equipe',
                    equipe_condition,
                ),
            )
            conditions.append(visibility)
        else:
            conditions.append(Observacao.escopo == 'global')

        query = (
            select(Observacao)
            .where(and_(*conditions))
            .options(
                selectinload(Observacao.mencoes),
                selectinload(Observacao.respostas).selectinload(Observacao.mencoes),
            )
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
        escopo = dados.escopo

        # Validacoes por escopo
        if escopo == 'equipe':
            if dados.equipe_id is None:
                raise HTTPException(
                    status_code=400,
                    detail="equipe_id e obrigatorio para observacoes de equipe",
                )
            membro_q = await db.execute(
                select(EquipeMembro).where(and_(
                    EquipeMembro.equipe_id == dados.equipe_id,
                    EquipeMembro.usuario == usuario,
                    EquipeMembro.deletado_em.is_(None),
                ))
            )
            if not membro_q.scalar_one_or_none():
                raise HTTPException(status_code=403, detail="Voce nao e membro desta equipe")

        equipe_id_salvar = dados.equipe_id if escopo == 'equipe' else None

        # Validar parent_id se fornecido
        if dados.parent_id is not None:
            parent_q = await db.execute(
                select(Observacao).where(and_(
                    Observacao.id == dados.parent_id,
                    Observacao.deletado_em.is_(None),
                ))
            )
            if not parent_q.scalar_one_or_none():
                raise HTTPException(status_code=404, detail="Observacao pai nao encontrada")

        observacao = Observacao(
            numero_processo=numero_limpo,
            usuario=usuario,
            conteudo=dados.conteudo,
            escopo=escopo,
            equipe_id=equipe_id_salvar,
            parent_id=dados.parent_id,
        )
        db.add(observacao)
        await db.flush()  # gera o id sem commitar ainda

        # Processar mencoes: combina as explicitas do frontend + extrai do conteudo
        mencoes_conteudo = _extrair_mencoes(dados.conteudo)
        todos_mencionados = list(dict.fromkeys(dados.mencoes + mencoes_conteudo))

        for usuario_mencionado in todos_mencionados:
            if usuario_mencionado == usuario:
                continue  # nao notifica o proprio autor
            mencao = ObservacaoMencao(
                observacao_id=observacao.id,
                usuario_mencionado=usuario_mencionado,
            )
            db.add(mencao)

        await db.commit()

        # Recarrega com os relacionamentos explicitamente (evita MissingGreenlet no async)
        result_reload = await db.execute(
            select(Observacao)
            .options(
                selectinload(Observacao.mencoes),
                selectinload(Observacao.respostas).selectinload(Observacao.mencoes),
            )
            .where(Observacao.id == observacao.id)
        )
        observacao = result_reload.scalar_one()

        logger.info(
            f"Observacao criada: processo={numero_limpo}, usuario={usuario}, "
            f"escopo={escopo}, mencoes={todos_mencionados}"
        )

        return {
            "status": "success",
            "data": ObservacaoResponse.model_validate(observacao),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao criar observacao: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.patch(
    "/{numero_processo}/{observacao_id}/visto",
    response_model=dict,
    summary="Marcar mencoes de um usuario como vistas em uma observacao",
)
async def marcar_visto(
    numero_processo: str,
    observacao_id: UUID,
    usuario: str = Query(..., description="Usuario que visualizou"),
    db: AsyncSession = Depends(get_db),
):
    try:
        numero_limpo = _strip_non_digits(numero_processo)
        agora = datetime.utcnow()

        # Busca a obs principal para validar que existe
        obs_q = await db.execute(
            select(Observacao).where(and_(
                Observacao.id == observacao_id,
                Observacao.numero_processo == numero_limpo,
                Observacao.deletado_em.is_(None),
            ))
        )
        obs = obs_q.scalar_one_or_none()
        if not obs:
            raise HTTPException(status_code=404, detail="Observacao nao encontrada")

        # IDs a marcar: a propria obs + respostas
        ids_para_marcar = [observacao_id]
        if obs.respostas:
            ids_para_marcar.extend([r.id for r in obs.respostas])

        # Marca visto_em nas mencoes do usuario nessas obs
        await db.execute(
            update(ObservacaoMencao)
            .where(and_(
                ObservacaoMencao.observacao_id.in_(ids_para_marcar),
                ObservacaoMencao.usuario_mencionado == usuario,
                ObservacaoMencao.visto_em.is_(None),
            ))
            .values(visto_em=agora)
        )
        await db.commit()

        logger.info(f"Mencoes marcadas como vistas: obs={observacao_id}, usuario={usuario}")

        return {"status": "success", "message": "Mencoes marcadas como vistas"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao marcar mencao como vista: {e}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@router.patch(
    "/{numero_processo}/{observacao_id}",
    response_model=dict,
    summary="Alterar observacao",
)
async def alterar_observacao(
    numero_processo: str,
    observacao_id: UUID,
    dados: ObservacaoUpdate,
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
            raise HTTPException(status_code=403, detail="Apenas o autor pode alterar a observacao")

        observacao.conteudo = dados.conteudo
        observacao.atualizado_em = datetime.utcnow()

        mencoes_conteudo = _extrair_mencoes(dados.conteudo)
        todos_mencionados = list(dict.fromkeys(dados.mencoes + mencoes_conteudo))
        mencionados_filtrados = [m for m in todos_mencionados if m != usuario]

        mencoes_existentes_q = await db.execute(
            select(ObservacaoMencao).where(ObservacaoMencao.observacao_id == observacao_id)
        )
        mencoes_existentes = mencoes_existentes_q.scalars().all()
        existentes_por_usuario = {m.usuario_mencionado: m for m in mencoes_existentes}
        novo_set = set(mencionados_filtrados)

        for usuario_mencionado, mencao in existentes_por_usuario.items():
            if usuario_mencionado not in novo_set:
                await db.delete(mencao)

        for usuario_mencionado in mencionados_filtrados:
            if usuario_mencionado not in existentes_por_usuario:
                db.add(
                    ObservacaoMencao(
                        observacao_id=observacao_id,
                        usuario_mencionado=usuario_mencionado,
                    )
                )

        await db.commit()

        result_reload = await db.execute(
            select(Observacao)
            .options(
                selectinload(Observacao.mencoes),
                selectinload(Observacao.respostas).selectinload(Observacao.mencoes),
            )
            .where(Observacao.id == observacao_id)
        )
        observacao = result_reload.scalar_one()

        logger.info(
            f"Observacao alterada: processo={numero_limpo}, observacao={observacao_id}, "
            f"usuario={usuario}, mencoes={mencionados_filtrados}"
        )

        return {"status": "success", "data": ObservacaoResponse.model_validate(observacao)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Erro ao alterar observacao: {e}")
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
