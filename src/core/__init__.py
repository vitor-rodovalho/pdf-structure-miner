"""
Configurações centrais e regras de negócio universais do domínio.
Guarda constantes imutáveis, mapeamentos de cabeçalho e variáveis de ambiente.
"""

from .constants import (
    FINANCIAL_TOTALS_KEYS,
    GARBAGE_CHARS,
    GARBAGE_DESC_WORDS,
    GENERIC_UNITS,
    HEADER_MAPPING,
    INVALID_DESC_PREFIXES,
    LOTE_BLOCK_WORDS,
    MAX_DESC_LEN,
    MAX_UNID_LEN,
    MAX_VALID_QUANTITY,
    MIN_DESC_LEN,
    MIN_DESC_NUMBER_LEN,
    TRASH_KEYWORDS,
    VALID_UNIDS_SET,
)
from .logger_config import setup_logging

__all__ = [
    "FINANCIAL_TOTALS_KEYS",
    "GARBAGE_CHARS",
    "GARBAGE_DESC_WORDS",
    "GENERIC_UNITS",
    "HEADER_MAPPING",
    "INVALID_DESC_PREFIXES",
    "LOTE_BLOCK_WORDS",
    "MAX_DESC_LEN",
    "MAX_UNID_LEN",
    "MAX_VALID_QUANTITY",
    "MIN_DESC_LEN",
    "MIN_DESC_NUMBER_LEN",
    "TRASH_KEYWORDS",
    "VALID_UNIDS_SET",
    "setup_logging",
]
