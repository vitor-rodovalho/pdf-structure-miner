import logging
from collections.abc import Iterator
from pathlib import Path

import docx
from docx.document import Document
from docx.table import Table, _Cell

from src.extractors import BaseExtractor, ExtractionState
from src.parsers import DOCXTableParser
from src.schemas import ItemLicitacao
from src.utils import deduplicate_items

logger = logging.getLogger(__name__)


class DocxExtractor(BaseExtractor):
    """
    Classe responsável por extrair dados de arquivos .DOCX usando a biblioteca python-docx.
    """

    # =========================================================================
    # EXTRAÇÃO E MÉTODOS AUXILIARES
    # =========================================================================

    def extract(self, file_path: Path) -> list[ItemLicitacao]:
        """
        Extrai os itens de licitação de um arquivo DOCX.

        Args:
            file_path (Path): Caminho do arquivo DOCX a ser processado.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos do DOCX.

        Raises:
            Exception: Qualquer erro durante a leitura ou processamento do arquivo.
        """
        if not file_path.exists() or not file_path.is_file():
            logger.error(f"Arquivo não encontrado ou inválido: {file_path}")
            return []

        extracted_items = []

        try:
            doc = docx.Document(str(file_path))
            logger.info(f"Iniciando DOCX: {file_path.name}")

            # Estado para regras de negócio entre linhas/páginas
            state: ExtractionState = {
                "last_lote": None,
                "item_counter": 1,
                "current_header_map": None,
                "pending_broken_desc": None,
                "last_extracted_item": None,
                "pending_item_num": None,
            }

            parser = DOCXTableParser()

            # Itera sobre todas as tabelas (incluindo aninhadas)
            for i, table in enumerate(self._iter_tables(doc)):
                # Converte objeto Table do Word para matriz de strings limpa
                table_data = self._table_to_list(table)

                if not table_data:
                    continue

                # Processa os dados da tabela
                items = parser.parse(table_data, state, table_index=i)
                extracted_items.extend(items)

            logger.info(
                f"Concluído DOCX: {file_path.name}. Itens brutos extraídos: {len(extracted_items)}"
            )

        except Exception as e:
            logger.error(f"Falha crítica ao ler o .DOCX '{file_path.name}': {e}", exc_info=True)

        return deduplicate_items(extracted_items)

    def _iter_tables(self, container: Document | _Cell) -> Iterator[Table]:
        """
        Generator recursivo para encontrar tabelas, inclusive aninhadas.

        Args:
            container (Document | _Cell): Pode ser um documento, célula ou tabela.

        Yields:
            Table: Tabelas encontradas no documento.
        """
        for table in container.tables:
            yield table
            # Procura tabelas aninhadas dentro das células
            for row in table.rows:
                for cell in row.cells:
                    yield from self._iter_tables(cell)

    def _table_to_list(self, table: Table) -> list[list[str]]:
        """
        Converte uma tabela do python-docx para uma lista de listas de strings.

        Args:
            table (Table): Objeto Table do python-docx.

        Returns:
            list[list[str]]: Matriz de strings representando o conteúdo da tabela.
        """
        data = []
        for row in table.rows:
            # List comprehension para extrair texto
            row_data = [cell.text.replace("\n", " ").strip() for cell in row.cells]

            if any(row_data):
                data.append(row_data)

        return data
