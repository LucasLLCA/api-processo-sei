"""
Script para popular a tabela tipos_documento a partir do endpoint documentos/tipos do SEI.

O script faz o login no SEI automaticamente usando as credenciais informadas e
em seguida busca todos os tipos de documento disponíveis, salvando no banco local.

Uso:
    python -m scripts.populate_tipos_documento
    python -m scripts.populate_tipos_documento --usuario SEU_EMAIL --senha SUA_SENHA --orgao SEAD-PI
    python -m scripts.populate_tipos_documento --dry-run   (só lista, não salva)
"""
import asyncio
import argparse
import sys
import os

import httpx

# Adiciona a raiz do projeto ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from api.database import engine, Base, AsyncSessionLocal
from api.models.tipo_documento import TipoDocumento
from api.config import settings
from sqlalchemy import text

SEI_BASE_URL = settings.SEI_BASE_URL


async def fazer_login(usuario: str, senha: str, orgao: str) -> tuple[str, str]:
    """
    Faz login no SEI e retorna (token, id_unidade).
    Usa o mesmo endpoint que o restante do sistema já usa.
    """
    url = f"{SEI_BASE_URL}/orgaos/usuarios/login"
    payload = {"Usuario": usuario, "Senha": senha, "Orgao": orgao}
    headers = {"accept": "application/json", "Content-Type": "application/json"}

    print(f"Fazendo login no SEI com usuário '{usuario}'...")

    async with httpx.AsyncClient(verify=False, timeout=120.0) as client:
        response = await client.post(url, json=payload, headers=headers)

    if response.status_code != 200:
        print(f"Erro no login — HTTP {response.status_code}: {response.text[:500]}")
        sys.exit(1)

    data = response.json()
    token = data.get("Token", "")
    unidades = data.get("Unidades", [])

    if not token:
        print("Erro: resposta do login não contém Token.")
        print(f"Resposta: {data}")
        sys.exit(1)

    if not unidades:
        print("Erro: resposta do login não contém Unidades.")
        sys.exit(1)

    id_unidade = str(unidades[0].get("Id", ""))
    sigla = unidades[0].get("Sigla", "")
    
    if len(unidades) > 1:
        print(f"\n⚠️  Múltiplas unidades encontradas:")
        for i, un in enumerate(unidades):
            sigla_un = un.get('Sigla', '')
            id_un = un.get('Id', '')
            # Marcar unidades sem "/" como administrativas (provavelmente raiz)
            eh_administrativa = "/" not in str(sigla_un)
            marker = "👑 ADMINISTRATIVA" if eh_administrativa else "📁 MUNICIPAL"
            print(f"   {i}: {sigla_un:30} (ID: {id_un:15}) {marker}")
        
        print("\n💡 Dica: Escolha a unidade ADMINISTRATIVA (👑) para ter TODOS os tipos disponíveis")
        print("   Unidades municipais (📁) têm apenas tipos dessa unidade")
        
        # Pedir para escolher
        escolha = input("\nQual unidade usar? (digite o índice): ").strip()
        try:
            idx = int(escolha)
            if 0 <= idx < len(unidades):
                id_unidade = str(unidades[idx].get("Id", ""))
                sigla = unidades[idx].get("Sigla", "")
                print(f"\n✅ Usando unidade {idx}: {sigla} (ID: {id_unidade})")
            else:
                print(f"❌ Índice inválido! Usando 0 por padrão.")
                id_unidade = str(unidades[0].get("Id", ""))
                sigla = unidades[0].get("Sigla", "")
        except ValueError:
            print(f"❌ Entrada inválida! Usando 0 por padrão.")
            id_unidade = str(unidades[0].get("Id", ""))
            sigla = unidades[0].get("Sigla", "")
    else:
        print(f"Login OK. Usando unidade: {sigla} (ID: {id_unidade})")
    
    return token, id_unidade


async def buscar_tipos_documento(token: str, id_unidade: str) -> list[dict]:
    """Busca todos os tipos de documento disponíveis no SEI (com paginação)."""
    url = f"{SEI_BASE_URL}/unidades/{id_unidade}/documentos/tipos"
    headers = {"accept": "application/json", "token": token}
    LIMIT = 100

    todos_tipos = []
    pagina = 1

    while True:
        # start = offset do registro (0, 100, 200, ...), não número da página
        offset = (pagina - 1) * LIMIT
        print(f"Buscando página {pagina} (offset {offset})...")
        params = {"limit": LIMIT, "start": offset}

        async with httpx.AsyncClient(verify=False, timeout=600.0) as client:
            response = await client.get(url, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Erro na página {pagina} — HTTP {response.status_code}: {response.text[:300]}")
            break

        data = response.json()

        # Primeira página: mostra estrutura real para debug
        if pagina == 1:
            print(f"  Estrutura da resposta: {list(data.keys()) if isinstance(data, dict) else type(data).__name__}")
            if isinstance(data, dict):
                for chave, val in data.items():
                    amostra = val[:2] if isinstance(val, list) and val else val
                    print(f"  [{chave}]: {amostra}")

        tipos = data.get("Tipos", [])
        if not tipos:
            break

        todos_tipos.extend(tipos)

        info = data.get("Info", {})
        total_paginas = info.get("TotalPaginas", 1)

        if pagina >= total_paginas:
            break
        pagina += 1

    print(f"Total de tipos encontrados: {len(todos_tipos)}")
    return todos_tipos


_MISSING = object()


def _pegar_campo(item: dict, *chaves) -> object:
    """Retorna o primeiro valor não-None encontrado entre as chaves informadas."""
    for chave in chaves:
        val = item.get(chave, _MISSING)
        if val is not _MISSING and val is not None and val != '':
            return val
    return None


def extrair_campos(item: dict) -> tuple[int, str]:
    """Extrai id e nome do item retornado pelo SEI."""
    id_val = _pegar_campo(item, "IdTipoDocumento", "id_tipo_documento", "Id", "id", "IdSerie")
    nome_val = _pegar_campo(item, "Nome", "nome", "Descricao", "descricao", "name")

    if id_val is None or nome_val is None:
        raise ValueError(f"Campos faltando — id={id_val}, nome={nome_val} — item: {item}")

    # Limpar prefixos: "ACADEPEN - Certificado" → "Certificado", "ADAPI.Atesto..." → "Atesto..."
    nome_limpo = str(nome_val).strip()
    
    # Trata padrão "PREFIX - DESCRIPTION"
    if " - " in nome_limpo:
        nome_limpo = nome_limpo.split(" - ", 1)[1].strip()
    
    # Trata padrão "PREFIX.DESCRIPTION"
    elif "." in nome_limpo:
        nome_limpo = nome_limpo.split(".", 1)[1].strip()
    
    return int(id_val), nome_limpo


async def populate(usuario: str, senha: str, orgao: str, dry_run: bool = False):
    token, id_unidade = await fazer_login(usuario, senha, orgao)
    tipos = await buscar_tipos_documento(token, id_unidade)

    print(f"\nTotal de tipos recebidos: {len(tipos)}")

    if not tipos:
        print("Nenhum tipo retornado pelo SEI. Encerrando.")
        return

    print("\nPrimeiros 5 tipos:")
    print(f"  DEBUG - Primeiro item completo: {tipos[0]}")
    for item in tipos[:5]:
        try:
            id_tipo, nome = extrair_campos(item)
            print(f"  [{id_tipo}] {nome}")
        except ValueError:
            print(f"  {item}")

    if dry_run:
        print("\n[dry-run] Nenhuma alteração salva no banco.")
        return

    # Garante que a tabela existe
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all, tables=[TipoDocumento.__table__])

    inserted = 0
    batch_size = 200

    # Deduplica por NOME (não por ID) — se o nome é igual, só salva uma vez
    tipos_unicos = {}
    for item in tipos:
        try:
            id_tipo, nome = extrair_campos(item)
            if nome not in tipos_unicos:  # Só adiciona se o nome ainda não existe
                tipos_unicos[nome] = id_tipo
        except ValueError as e:
            print(f"  [aviso] Pulando item inválido: {e}")
            continue
    
    print(f"Após deduplicação: {len(tipos_unicos)} tipos únicos")

    async with AsyncSessionLocal() as session:
        await session.execute(text("DELETE FROM tipos_documento"))
        await session.commit()

        items = list(tipos_unicos.items())
        for i in range(0, len(items), batch_size):
            batch = items[i : i + batch_size]
            
            # Usa ON CONFLICT para evitar erro se houver duplicatas
            stmt = text("""
                INSERT INTO tipos_documento (id_tipo_documento, nome)
                VALUES (:id, :nome)
                ON CONFLICT (id_tipo_documento) DO UPDATE
                SET nome = EXCLUDED.nome
            """)
            
            for nome, id_tipo in batch:
                await session.execute(stmt, {"id": id_tipo, "nome": nome})
            
            await session.commit()
            inserted += len(batch)
            print(f"  Processados {inserted}/{len(items)}...")

    print(f"\nConcluído! {inserted} tipos de documento salvos no banco.")


def main():
    parser = argparse.ArgumentParser(description="Popula tipos_documento a partir do SEI")
    parser.add_argument("--usuario", default="gabriel.coelho@sead.pi.gov.br", help="Usuário SEI (e-mail)")
    parser.add_argument("--senha",   default="fenek3161@SEAD",               help="Senha SEI")
    parser.add_argument("--orgao",   default="SEAD-PI",                       help="Órgão SEI (ex: SEAD-PI)")
    parser.add_argument("--dry-run", action="store_true",                     help="Apenas lista, não salva")
    args = parser.parse_args()

    asyncio.run(populate(args.usuario, args.senha, args.orgao, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
