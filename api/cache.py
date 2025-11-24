import redis.asyncio as aioredis
import orjson
import logging
from typing import Optional, Any
from .config import settings

logger = logging.getLogger(__name__)


class RedisCache:
    """
    Cliente Redis assíncrono para cache da API SEI
    """
    def __init__(self):
        self.redis_client = None
        self._connected = False

    async def connect(self):
        """Conecta ao Redis com tratamento de erros"""
        if self._connected:
            return

        try:
            # Prepara URL de conexão
            if settings.REDIS_PASSWORD:
                redis_url = f"redis://{settings.REDIS_USERNAME}:{settings.REDIS_PASSWORD}@{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"
            else:
                redis_url = f"redis://{settings.REDIS_HOST}:{settings.REDIS_PORT}/{settings.REDIS_DB}"

            self.redis_client = aioredis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                max_connections=20
            )

            # Testa a conexão
            await self.redis_client.ping()
            self._connected = True
            logger.info("Conexão com Redis estabelecida com sucesso")
        except Exception as e:
            logger.warning(f"Não foi possível conectar ao Redis: {str(e)}")
            self.redis_client = None
            self._connected = False

    async def close(self):
        """Fecha a conexão com o Redis"""
        if self.redis_client:
            await self.redis_client.close()
            self._connected = False

    async def is_available(self) -> bool:
        """Verifica se o Redis está disponível"""
        if self.redis_client is None:
            return False
        try:
            await self.redis_client.ping()
            return True
        except Exception:
            return False

    async def get(self, key: str) -> Optional[Any]:
        """
        Obtém um valor do cache

        Args:
            key: Chave do cache

        Returns:
            Valor do cache ou None se não encontrado ou em caso de erro
        """
        if not self._connected:
            await self.connect()

        if self.redis_client is None:
            return None

        try:
            value = await self.redis_client.get(key)
            if value:
                logger.debug(f"[CACHE HIT] Chave: {key}")
                return orjson.loads(value)
            logger.debug(f"[CACHE MISS] Chave: {key}")
            return None
        except Exception as e:
            logger.warning(f"Erro ao obter cache para chave {key}: {str(e)}")
            return None

    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """
        Define um valor no cache

        Args:
            key: Chave do cache
            value: Valor a ser armazenado
            ttl: Tempo de expiração em segundos (padrão: 1 hora)

        Returns:
            True se sucesso, False caso contrário
        """
        if not self._connected:
            await self.connect()

        if self.redis_client is None:
            return False

        try:
            serialized_value = orjson.dumps(value).decode('utf-8')
            await self.redis_client.setex(key, ttl, serialized_value)
            logger.debug(f"[CACHE SET] Chave: {key}, TTL: {ttl}s")
            return True
        except Exception as e:
            logger.warning(f"Erro ao definir cache para chave {key}: {str(e)}")
            return False

    async def delete(self, key: str) -> bool:
        """
        Remove um valor do cache

        Args:
            key: Chave do cache

        Returns:
            True se sucesso, False caso contrário
        """
        if not self._connected:
            await self.connect()

        if self.redis_client is None:
            return False

        try:
            await self.redis_client.delete(key)
            logger.debug(f"[CACHE DELETE] Chave: {key}")
            return True
        except Exception as e:
            logger.warning(f"Erro ao deletar cache para chave {key}: {str(e)}")
            return False

    async def clear_pattern(self, pattern: str) -> int:
        """
        Remove todas as chaves que correspondem ao padrão usando SCAN (não bloqueia)

        Args:
            pattern: Padrão de chave (ex: "processo:*")

        Returns:
            Número de chaves removidas
        """
        if not self._connected:
            await self.connect()

        if self.redis_client is None:
            return 0

        try:
            deleted = 0
            cursor = 0
            while True:
                cursor, keys = await self.redis_client.scan(cursor, match=pattern, count=100)
                if keys:
                    deleted += await self.redis_client.delete(*keys)
                if cursor == 0:
                    break
            logger.debug(f"[CACHE CLEAR] Padrão: {pattern}, Chaves removidas: {deleted}")
            return deleted
        except Exception as e:
            logger.warning(f"Erro ao limpar cache com padrão {pattern}: {str(e)}")
            return 0

    async def get_keys(self, pattern: str = "*") -> list:
        """
        Lista chaves que correspondem ao padrão usando SCAN

        Args:
            pattern: Padrão de chave (ex: "processo:*")

        Returns:
            Lista de chaves
        """
        if not self._connected:
            await self.connect()

        if self.redis_client is None:
            return []

        try:
            keys = []
            cursor = 0
            while True:
                cursor, batch = await self.redis_client.scan(cursor, match=pattern, count=100)
                keys.extend(batch)
                if cursor == 0:
                    break
            return keys
        except Exception as e:
            logger.warning(f"Erro ao listar chaves com padrão {pattern}: {str(e)}")
            return []

    async def get_info(self) -> dict:
        """
        Obtém informações do Redis

        Returns:
            Dicionário com informações do Redis
        """
        if not self._connected:
            await self.connect()

        if self.redis_client is None:
            return {}

        try:
            info = await self.redis_client.info()
            return {
                "connected_clients": info.get("connected_clients", 0),
                "used_memory_human": info.get("used_memory_human", "0B"),
                "total_connections_received": info.get("total_connections_received", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0)
            }
        except Exception as e:
            logger.warning(f"Erro ao obter informações do Redis: {str(e)}")
            return {}


# Instância global do cache
cache = RedisCache()


def gerar_chave_processo(numero_processo: str, id_primeiro_doc: str, id_ultimo_doc: str = None) -> str:
    """
    Gera chave de cache para entendimento de processo

    Args:
        numero_processo: Número do processo
        id_primeiro_doc: ID do primeiro documento
        id_ultimo_doc: ID do último documento (opcional)

    Returns:
        Chave formatada para cache
    """
    if id_ultimo_doc:
        return f"processo:{numero_processo}:primeiro:{id_primeiro_doc}:ultimo:{id_ultimo_doc}"
    return f"processo:{numero_processo}:primeiro:{id_primeiro_doc}"


def gerar_chave_documento(documento_formatado: str) -> str:
    """
    Gera chave de cache para resumo de documento

    Args:
        documento_formatado: ID formatado do documento

    Returns:
        Chave formatada para cache
    """
    return f"documento:{documento_formatado}"


def gerar_chave_andamento(numero_processo: str) -> str:
    """
    Gera chave de cache para andamento de processo

    Args:
        numero_processo: Número do processo

    Returns:
        Chave formatada para cache
    """
    return f"andamento:{numero_processo}"


def gerar_chave_resumo(numero_processo: str) -> str:
    """
    Gera chave de cache para resumo de processo

    Args:
        numero_processo: Número do processo

    Returns:
        Chave formatada para cache
    """
    return f"resumo:{numero_processo}"
