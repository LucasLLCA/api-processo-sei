#!/usr/bin/env python3
"""
Invalidate entendimento (resumo_completo) and situação atual cache for a processo.

Usage:
    python invalidate_cache.py <numero_processo>
    python invalidate_cache.py 00002010280202349

Environment variables (optional):
    REDIS_HOST     (default: redis)
    REDIS_PORT     (default: 6379)
    REDIS_DB       (default: 0)
    REDIS_USERNAME (default: default)
    REDIS_PASSWORD (default: "")
"""

import asyncio
import os
import re
import sys

import redis.asyncio as aioredis


def normalizar(numero: str) -> str:
    return re.sub(r"\D", "", numero)


async def main():
    if len(sys.argv) < 2:
        print(__doc__.strip())
        sys.exit(1)

    numero = normalizar(sys.argv[1])
    if not numero:
        print("Erro: número do processo inválido.")
        sys.exit(1)

    host = os.getenv("REDIS_HOST", "10.0.122.91")
    port = int(os.getenv("REDIS_PORT", "6379"))
    db = int(os.getenv("REDIS_DB", "0"))
    username = os.getenv("REDIS_USERNAME", "default")
    password = os.getenv("REDIS_PASSWORD", "cJmvC04noB7NkV9MonHIG6INCDK5PTxh1LoJyDITMiCXiG5EFaDsCvGV7UXWsnA5")

    if password:
        url = f"redis://{username}:{password}@{host}:{port}/{db}"
    else:
        url = f"redis://{host}:{port}/{db}"

    client = aioredis.from_url(url, encoding="utf-8", decode_responses=True)

    keys_to_delete = [
        f"processo:{numero}:resumo_completo",
        f"processo:{numero}:situacao_atual",
    ]

    try:
        await client.ping()
        print(f"Conectado ao Redis em {host}:{port}/{db}")
    except Exception as e:
        print(f"Erro ao conectar ao Redis: {e}")
        sys.exit(1)

    deleted = 0
    for key in keys_to_delete:
        result = await client.delete(key)
        if result:
            print(f"  Deletado: {key}")
            deleted += 1
        else:
            print(f"  Não encontrado: {key}")

    print(f"\nProcesso {numero}: {deleted} chave(s) invalidada(s).")
    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
