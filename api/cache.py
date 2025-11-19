import redis
import json
from typing import Optional, Any
from .config import settings

class RedisCache:
    """
    Cliente Redis para cache da API SEI
    """
    def __init__(self):
        self.redis_client = None
        self._connect()

    def _connect(self):
        """Conecta ao Redis com tratamento de erros"""
        try:
            # Prepara parâmetros de conexão
            redis_params = {
                "host": settings.REDIS_HOST,
                "port": settings.REDIS_PORT,
                "db": settings.REDIS_DB,
                "decode_responses": True,
                "socket_connect_timeout": 5,
                "socket_timeout": 5
            }

            # Adiciona username se fornecido (Redis 6+)
            if settings.REDIS_USERNAME:
                redis_params["username"] = settings.REDIS_USERNAME

            # Adiciona password se fornecido
            if settings.REDIS_PASSWORD:
                redis_params["password"] = settings.REDIS_PASSWORD

            self.redis_client = redis.Redis(**redis_params)

            # Testa a conexão
            self.redis_client.ping()
            print("[INFO] Conexão com Redis estabelecida com sucesso")
        except Exception as e:
            print(f"[WARN] Não foi possível conectar ao Redis: {str(e)}")
            self.redis_client = None

    def is_available(self) -> bool:
        """Verifica se o Redis está disponível"""
        if self.redis_client is None:
            return False
        try:
            self.redis_client.ping()
            return True
        except Exception:
            return False

    def get(self, key: str) -> Optional[Any]:
        """
        Obtém um valor do cache

        Args:
            key: Chave do cache

        Returns:
            Valor do cache ou None se não encontrado ou em caso de erro
        """
        if not self.is_available():
            return None

        try:
            value = self.redis_client.get(key)
            if value:
                print(f"[CACHE HIT] Chave: {key}")
                return json.loads(value)
            print(f"[CACHE MISS] Chave: {key}")
            return None
        except Exception as e:
            print(f"[WARN] Erro ao obter cache para chave {key}: {str(e)}")
            return None

    def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """
        Define um valor no cache

        Args:
            key: Chave do cache
            value: Valor a ser armazenado
            ttl: Tempo de expiração em segundos (padrão: 1 hora)

        Returns:
            True se sucesso, False caso contrário
        """
        if not self.is_available():
            return False

        try:
            serialized_value = json.dumps(value, ensure_ascii=False)
            self.redis_client.setex(key, ttl, serialized_value)
            print(f"[CACHE SET] Chave: {key}, TTL: {ttl}s")
            return True
        except Exception as e:
            print(f"[WARN] Erro ao definir cache para chave {key}: {str(e)}")
            return False

    def delete(self, key: str) -> bool:
        """
        Remove um valor do cache

        Args:
            key: Chave do cache

        Returns:
            True se sucesso, False caso contrário
        """
        if not self.is_available():
            return False

        try:
            self.redis_client.delete(key)
            print(f"[CACHE DELETE] Chave: {key}")
            return True
        except Exception as e:
            print(f"[WARN] Erro ao deletar cache para chave {key}: {str(e)}")
            return False

    def clear_pattern(self, pattern: str) -> int:
        """
        Remove todas as chaves que correspondem ao padrão

        Args:
            pattern: Padrão de chave (ex: "processo:*")

        Returns:
            Número de chaves removidas
        """
        if not self.is_available():
            return 0

        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted = self.redis_client.delete(*keys)
                print(f"[CACHE CLEAR] Padrão: {pattern}, Chaves removidas: {deleted}")
                return deleted
            return 0
        except Exception as e:
            print(f"[WARN] Erro ao limpar cache com padrão {pattern}: {str(e)}")
            return 0


# Instância global do cache
cache = RedisCache()


def gerar_chave_processo(numero_processo: str, id_primeiro_doc: str, id_ultimo_doc: str) -> str:
    """
    Gera chave de cache para entendimento de processo

    Args:
        numero_processo: Número do processo
        id_primeiro_doc: ID do primeiro documento
        id_ultimo_doc: ID do último documento

    Returns:
        Chave formatada para cache
    """
    return f"processo:{numero_processo}:primeiro:{id_primeiro_doc}:ultimo:{id_ultimo_doc}"


def gerar_chave_documento(documento_formatado: str) -> str:
    """
    Gera chave de cache para resumo de documento

    Args:
        documento_formatado: ID formatado do documento

    Returns:
        Chave formatada para cache
    """
    return f"documento:{documento_formatado}"
