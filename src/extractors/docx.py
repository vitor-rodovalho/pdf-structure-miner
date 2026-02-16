import logging
from collections.abc import Iterator
from pathlib import Path

import docx
from docx.document import Document
from docx.table import Table, _Cell

from src.extractors.base import BaseExtractor, ExtractionState
from src.schemas.licitacao import ItemLicitacao

logger = logging.getLogger(__name__)


class DocxExtractor(BaseExtractor):
    """
    Classe responsável por extrair dados de arquivos DOCX usando a biblioteca python-docx.
    """

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
        extracted_items = []

        try:
            doc = docx.Document(str(file_path))
            logger.info(f"Iniciando DOCX: {file_path.name}")

            # Estado para regras de negócio entre linhas/páginas
            state: ExtractionState = {"last_lote": None, "item_counter": 1}

            # Itera sobre todas as tabelas (incluindo aninhadas)
            for table in self._iter_tables(doc):
                # Converte objeto Table do Word para matriz de strings limpa
                table_data = self._table_to_list(table)

                if not table_data:
                    continue

                # Processa os dados da tabela
                items = self._process_table_data(table_data, state)
                extracted_items.extend(items)

        except Exception as e:
            logger.error(f"Erro ao processar DOCX {file_path}: {e}")

        return extracted_items


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
            row_data = [cell.text.strip() for cell in row.cells]
            data.append(row_data)
        return data


    def _process_table_data(
        self, data: list[list[str]], state: ExtractionState
    ) -> list[ItemLicitacao]:
        """
        Identifica cabeçalhos e extrai itens de uma matriz de dados.

        Args:
            data (list[list[str]]): Matriz de strings representando o conteúdo da tabela.
            state (dict): Dicionário para manter estado entre linhas/páginas, como último lote e
            contador de itens.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos da tabela.
        """
        items = []

        # Tenta identificar cabeçalho na primeira linha
        header_map = self._identify_columns(data[0])

        if not header_map:
            return []

        # Se achou cabeçalho, começa da linha 1
        start_idx = 1

        for row in data[start_idx:]:
            # Usa o método da classe pai (BaseExtractor)
            item = self._parse_row(row, header_map, state)
            if item:
                items.append(item)

        return items
