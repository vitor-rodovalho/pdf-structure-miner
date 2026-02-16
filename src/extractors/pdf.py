import logging
from collections.abc import Sequence
from pathlib import Path

import pdfplumber

from src.extractors.base import BaseExtractor, ExtractionState
from src.schemas.licitacao import ItemLicitacao

logger = logging.getLogger(__name__)


class PDFExtractor(BaseExtractor):
    """
    Classe responsável por extrair dados de arquivos PDF usando a biblioteca pdfplumber.
    """

    MIN_DESCRIPTION_LENGTH = 3  # Descrições muito curtas provavelmente são ruído

    def extract(self, file_path: Path) -> list[ItemLicitacao]:
        """
        Extrai os itens de licitação de um arquivo PDF.

        Args:
            file_path (Path): Caminho do arquivo PDF a ser processado.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos do PDF.

        Raises:
            Exception: Qualquer erro durante a leitura ou processamento do arquivo.
        """

        extracted_items = []

        try:
            with pdfplumber.open(file_path) as pdf:
                logger.info(f"Iniciando: {file_path.name} ({len(pdf.pages)} págs)")

                # Tentativa de extração via tabelas estruturadas
                table_items = self._extract_from_tables(pdf)
                extracted_items.extend(table_items)

                # Tentativa de extração via texto corrido (Fallback)
                if not extracted_items or "-relacaoitens" in file_path.name.lower():
                    logger.info("Tentando extração via Texto (Layout Formulário)...")
                    text_items = self._extract_text_items(pdf)

                    if text_items:
                        extracted_items.extend(text_items)
                        logger.info(f"Extração via texto recuperou {len(text_items)} item(s).")

        except Exception as e:
            logger.error(f"Erro ao processar PDF {file_path}: {e}")
        return extracted_items


    def _extract_from_tables(self, pdf) -> list[ItemLicitacao]:
        """
        Tenta extrair itens varrendo tabelas estruturadas nas páginas.

        Args:
            pdf: Objeto PDF aberto pelo pdfplumber.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos das tabelas.
        """

        items = []
        state: ExtractionState = {"last_lote": None, "item_counter": 1}

        for page in pdf.pages:
            tables = page.extract_tables()
            if not tables:
                continue

            for table in tables:
                if not table:
                    continue

                header_map = self._identify_columns(table[0])
                if not header_map:
                    continue

                # Pula o cabeçalho
                for row in table[1:]:
                    item = self._parse_row(row, header_map, state)
                    if item:
                        items.append(item)
        return items


    def _extract_text_items(self, pdf) -> list[ItemLicitacao]:
        """
        Varre o texto do PDF procurando por padrões de formulário (Chave: Valor).

        Args:
            pdf: Objeto PDF aberto pelo pdfplumber.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos do texto.
        """

        items = []
        full_text = ""

        # Concatena todo o texto para facilitar a varredura contínua
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n"

        lines = full_text.split("\n")

        # Estado do loop
        current_item = {}
        current_lote = None

        for i, raw_line in enumerate(lines):
            line = raw_line.strip()
            if not line:
                continue

            line_lower = line.lower()

            # Regra: Começa com digito, tem hífen e tem palavras-chave de agrupamento
            is_header_pattern = line[0].isdigit() and "-" in line
            is_group_keyword = any(x in line_lower for x in ["grupo", "lote", "itens da licitação"])

            if is_header_pattern and is_group_keyword:
                # Se mudou o lote, o item anterior com certeza acabou
                self._flush_item(current_item, items, current_lote)

                parts = line.split("-", 1)
                current_lote = parts[0].strip()

                # Não processa essa linha como texto de item
                continue

            # Processa o conteúdo do item
            self._process_text_line(line, lines, i, current_item, items, current_lote)

        # Flush final
        self._flush_item(current_item, items, current_lote)
        return items


    def _process_text_line(
        self,
        line: str,
        all_lines: Sequence[str],
        idx: int,
        current_item: dict,
        items: list,
        current_lote: str | None,
    ):
        """
        Processa uma única linha de texto e atualiza o estado do item.

        Args:
            line (str): Linha atual de texto a ser processada.
            all_lines (Sequence[str]): Lista de todas as linhas do texto para referência.
            idx (int): Índice da linha atual para buscar a próxima linha se necessário.
            current_item (dict): Dicionário com os dados do item em construção.
            items (list): Lista onde o item finalizado deve ser adicionado.
            current_lote (str | None): Lote atual, se identificado.
        """

        line_lower = line.lower()

        # Captura Descrição
        if "descrição" in line.lower() or "objeto:" in line.lower():
            self._handle_description(line, all_lines, idx, current_item, items, current_lote)

        # Captura Quantidade
        elif "quantidade" in line.lower() and ":" in line:
            # Evita confundir "Local de Entrega (Quantidade)" com a quantidade do item
            if "local de entrega" not in line_lower:
                val_str = self._extract_value_after_colon(line, all_lines, idx)
                qtd = self._clean_number(val_str)
                if qtd:
                    current_item["quantidade"] = int(qtd)

        # Captura Unidade
        elif "unidade" in line.lower() and ":" in line:
            val_str = self._extract_value_after_colon(line, all_lines, idx)
            if val_str:
                current_item["unidade_fornecimento"] = val_str

        # Captura fim de item
        elif "local de entrega" in line_lower:
            self._flush_item(current_item, items, current_lote)


    def _handle_description(
        self,
        line: str,
        all_lines: Sequence[str],
        current_idx: int,
        current_item: dict,
        items_list: list,
        current_lote: str | None,
    ):
        """
        Processa a linha de descrição, salvando o item anterior se necessário.

        Args:
            line (str): Linha atual onde se espera encontrar a descrição.
            all_lines (Sequence[str]): Lista de todas as linhas do texto para referência.
            current_idx (int): Índice da linha atual para buscar a próxima linha se necessário.
            current_item (dict): Dicionário com os dados do item em construção.
            items_list (list): Lista onde o item finalizado deve ser adicionado.
            current_lote (str | None): Lote atual, se identificado.
        """

        # Se já havia um item completo, salva
        if "objeto" in current_item and "quantidade" in current_item:
            self._save_buffer_item(items_list, current_item, current_lote)
            current_item.clear()  # Limpa o dicionário in-place

        # Extrai a nova descrição
        desc = self._extract_value_after_colon(line, all_lines, current_idx)
        if desc:
            current_item["objeto"] = desc


    def _extract_value_after_colon(
        self, line: str, all_lines: Sequence[str], current_idx: int
    ) -> str:
        """
        Pega o texto após ':', ou na linha seguinte se estiver vazio.

        Args:
            line (str): Linha atual onde se espera encontrar a chave e o valor.
            all_lines (Sequence[str]): Lista de todas as linhas do texto para referência.
            current_idx (int): Índice da linha atual para buscar a próxima linha se necessário.

        Returns:
            str: O valor extraído, ou string vazia se não encontrado.
        """

        parts = line.split(":", 1)

        # Valor na mesma linha
        if len(parts) > 1 and parts[1].strip():
            return parts[1].strip()

        # Valor na linha de baixo
        elif current_idx + 1 < len(all_lines):
            return all_lines[current_idx + 1].strip()

        return ""


    def _flush_item(self, current_item: dict, items_list: list, current_lote: str | None):
        """
        Salva o item atual e limpa o buffer.

        Args:
            current_item (dict): Dicionário com os dados do item em construção.
            items_list (list): Lista onde o item finalizado deve ser adicionado.
            current_lote (str | None): Lote atual, se identificado.
        """

        if "objeto" in current_item and "quantidade" in current_item:
            self._save_buffer_item(items_list, current_item, current_lote)
            current_item.clear()


    def _save_buffer_item(self, items_list: list, buffer: dict, lote: str | None):
        """
        Helper para criar o objeto ItemLicitacao e adicionar na lista.

        Args:
            items_list (list): Lista onde o item finalizado deve ser adicionado.
            buffer (dict): Dicionário com os dados do item em construção.
            lote (str | None): Lote atual, se identificado.
        """

        try:
            # Garante que há a quantidade antes de tentar criar o objeto
            qtd_raw = buffer.get("quantidade")
            if qtd_raw is None:
                return

            # Gera número sequencial simples se não foi extraído
            next_id = len(items_list) + 1

            desc = buffer.get("objeto", "").replace("Detalhada:", "").strip()

            if len(desc) < self.MIN_DESCRIPTION_LENGTH:
                return

            new_item = ItemLicitacao(
                item=next_id,
                objeto=desc,
                quantidade=int(qtd_raw),
                unidade_fornecimento=buffer.get("unidade_fornecimento", "UN"),
                lote=lote,
            )
            items_list.append(new_item)

        except Exception as e:
            logger.debug(f"Falha ao criar item do texto: {e} | Dados: {buffer}")
