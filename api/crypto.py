"""
Utilitários de criptografia Fernet para credenciais armazenadas.
"""
from cryptography.fernet import Fernet

from .config import settings

_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        if not settings.FERNET_KEY:
            raise RuntimeError("FERNET_KEY não configurado")
        _fernet = Fernet(settings.FERNET_KEY.encode())
    return _fernet


def encrypt_password(plaintext: str) -> str:
    return _get_fernet().encrypt(plaintext.encode()).decode()


def decrypt_password(ciphertext: str) -> str:
    return _get_fernet().decrypt(ciphertext.encode()).decode()
