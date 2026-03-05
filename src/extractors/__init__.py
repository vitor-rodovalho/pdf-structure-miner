"""
Pacote responsável pela extração de dados de arquivos de licitação.
Contém os extratores específicos para diferentes formatos (PDF, DOCX) e a interface base para
padronização.
"""

from src.extractors.base import BaseExtractor, ExtractionState
from src.extractors.docx import DocxExtractor
from src.extractors.pdf import PDFExtractor

__all__ = ["BaseExtractor", "DocxExtractor", "ExtractionState", "PDFExtractor"]
