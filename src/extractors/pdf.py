import logging
from pathlib import Path

import pdfplumber
from pdfplumber.pdf import PDF

from src.extractors import BaseExtractor, ExtractionState
from src.parsers import PDFTableParser, RelacaoItensParser
from src.schemas import ItemLicitacao
from src.utils import deduplicate_items

logger = logging.getLogger(__name__)


class PDFExtractor(BaseExtractor):
    """
    Extrator Híbrido com Modo Especialista para 'Relação de Itens'.
    """

    # =========================================================================
    # EXTRAÇÃO E MÉTODOS AUXILIARES
    # =========================================================================

    def extract(self, file_path: Path) -> list[ItemLicitacao]:
        """
        Extrai itens de um arquivo PDF, aplicando uma estratégia híbrida que inclui um modo
        especialista para documentos do tipo 'Relação de Itens', uma abordagem tradicional de
        extração de tabelas e um fallback para casos problemáticos.

        Args:
            file_path (Path): Caminho para o arquivo PDF a ser processado.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos do PDF.
        """
        if not file_path.exists() or not file_path.is_file():
            logger.error(f"Arquivo não encontrado ou inválido: {file_path}")
            return []

        extracted = []
        try:
            with pdfplumber.open(file_path) as pdf:
                logger.info(f"Iniciando PDF: {file_path.name} ({len(pdf.pages)} págs)")

                # Obtém o texto completo para análise
                full_text = ""
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        full_text += t + "\n"

                # Verifica se o documento é uma 'Relação de Itens' antes de tentar extrair tabelas
                if self._is_relacao_itens(full_text):
                    logger.info("Detectado formato 'Relação de Itens'. Usando parser específico.")
                    parser = RelacaoItensParser()

                    itens_relacao = parser.parse(full_text)
                    logger.info(
                        f"Concluído PDF (Relação de Itens): {file_path.name}. "
                        f"Itens extraídos: {len(itens_relacao)}"
                    )

                    return itens_relacao

                # Tenta extrair usando a estratégia tradicional de tabelas
                logger.debug(f"[{file_path.name}] Extraindo tabelas.")
                extracted = self._extract_tables(pdf)

        except Exception as e:
            logger.error(f"Falha crítica ao ler o .PDF '{file_path.name}': {e}", exc_info=True)

        final_items = deduplicate_items(extracted)

        logger.info(f"Concluído PDF: {file_path.name}. Itens brutos extraídos: {len(final_items)}")

        return final_items

    def _is_relacao_itens(self, text: str) -> bool:
        """
        Verifica se o documento é uma Relação de Itens oficial.

        Busca por padrões específicos como "Relação de Itens", "Pregão" e "1 - itens da licitação"
        nos primeiros 1000 caracteres do documento.

        Args:
            text (str): Texto completo extraído do PDF.

        Returns:
            bool: True se o documento for identificado como Relação de Itens, False caso contrário.
        """

        head = text[:1000].lower()

        has_titulo = "relação de itens" in head
        has_pregao = "pregão" in head or "licitação" in head
        has_marcador_itens = "1 - itens da licitação" in head

        return has_titulo and has_pregao and has_marcador_itens

    def _extract_tables(self, pdf: PDF) -> list[ItemLicitacao]:
        """
        Lê e extrai dados das tabelas do documento PDF fornecido.

        Args:
            pdf (PDF): Objeto PDFplumber já aberto e carregado.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos usando a estratégia tradicional.
        """
        items = []
        state: ExtractionState = {
            "last_lote": None,
            "item_counter": 1,
            "current_header_map": None,
            "pending_broken_desc": None,
            "last_extracted_item": None,
            "pending_item_num": None,
        }

        parser = PDFTableParser()

        for page in pdf.pages:
            tables = page.extract_tables()

            # Se a página não tem linhas de tabela, apenas pula
            if not tables:
                continue

            for table in tables:
                if table:
                    items.extend(parser.parse_table(table, state))

        return items
