"""
Motores de interpretação e extração de dados (Parsers).

Este pacote é responsável por receber dados textuais ou semi-estruturados (entregues pelos
extratores) e aplicar regras lógicas e heurísticas para transformá-los em objetos de domínio
consolidados (ItemLicitacao).
"""

from .docx_parser import DOCXTableParser
from .pdf_table_parser import ExtractionState, PDFTableParser
from .relacao_itens_parser import RelacaoItensParser

__all__ = [
    "DOCXTableParser",
    "ExtractionState",
    "PDFTableParser",
    "RelacaoItensParser",
]
