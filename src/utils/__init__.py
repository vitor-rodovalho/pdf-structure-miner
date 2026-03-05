"""
Funções utilitárias puras e ferramentas de higienização.
Este pacote centraliza lógicas de limpeza de texto, formatação de números e tratamento de strings,
sem dependências de estado.
"""

from .cleaners import (
    MAX_ALPHA_IN_NUMBER,
    MAX_UNID_LEN,
    clean_number,
    clean_rows,
    clean_unidade_fornecimento,
    get_text_safe,
    normalize_lote,
)
from .deduplicator import deduplicate_items

__all__ = [
    "MAX_ALPHA_IN_NUMBER",
    "MAX_UNID_LEN",
    "clean_number",
    "clean_rows",
    "clean_unidade_fornecimento",
    "deduplicate_items",
    "get_text_safe",
    "normalize_lote",
]
