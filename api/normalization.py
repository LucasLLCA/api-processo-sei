"""
Funções de normalização para números de processo SEI.

Aceita tanto formato com pontuação (00002.012041/2025-95)
quanto apenas dígitos (00002012041202595).
"""
import re


def normalizar_numero_processo(numero: str) -> str:
    """Remove todos os caracteres não-numéricos de um número de processo."""
    return re.sub(r'\D', '', numero)


def formatar_numero_processo(numero: str) -> str:
    """
    Formata um número de processo de 17 dígitos no padrão NNNNN.NNNNNN/AAAA-DD.

    Se o número não tiver exatamente 17 dígitos, retorna o valor original.
    """
    limpo = re.sub(r'\D', '', numero)
    if len(limpo) != 17:
        return numero
    return f"{limpo[:5]}.{limpo[5:11]}/{limpo[11:15]}-{limpo[15:17]}"
