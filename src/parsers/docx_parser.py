import logging
import re
from collections.abc import Sequence

from unidecode import unidecode

from src.core.constants import (
    HEADER_MAPPING,
    INVALID_DESC_PREFIXES,
    LOTE_BLOCK_WORDS,
    MIN_DESC_LEN,
    VALID_UNIDS_SET,
)
from src.extractors import ExtractionState
from src.schemas import ItemLicitacao
from src.utils import (
    clean_number,
    clean_unidade_fornecimento,
    get_text_safe,
    normalize_lote,
)

logger = logging.getLogger(__name__)


class DOCXTableParser:
    """
    Motor especializado em interpretar matrizes extraídas de arquivos Word (DOCX).
    Possui heurísticas exclusivas para lidar com mesclagens invisíveis e deslocamentos de colunas.
    """

    # =========================================================================
    # CONSTANTES ESPECÍFICAS DE DOCX E CABEÇALHOS
    # =========================================================================

    # Máximo de caracteres em uma célula para ser considerada parte de um título de coluna
    MAX_HEADER_CELL_LENGTH = 60

    # Quantidade mínima de colunas identificadas para confirmar que a linha é um cabeçalho
    HEADER_MATCH_THRESHOLD = 2

    # Mínimo de colunas validadas necessárias para inicializar o mapeamento do extrator
    MIN_COLUMNS_FOR_HEADER = 2

    # Colunas sensíveis que exigem validação rigorosa para não sofrerem falsos positivos
    RESTRICTED_COLUMN_KEYS = frozenset({"item", "quantidade"})

    # Termos (geralmente financeiros ou de unidade) que anulam a detecção das colunas restritas
    FORBIDDEN_COLUMN_TERMS = ("vlr", "valor", "preco", "preço", "total", "r$", "unit")

    # Agrupamento lógico das palavras-chave para busca de cabeçalho na linha inteira concatenada
    HEADER_KEYWORD_GROUPS = (
        ("item", "código"),
        ("objeto", "descri", "especific"),
        ("quant", "qtd"),
        ("unid", "und"),
    )

    # Tamanho mínimo exigido para resgatar uma descrição perdida usando a maior string da linha
    MIN_DESC_FALLBACK_LEN = 15

    # Tamanho máximo que uma string pode ter para ser avaliada como uma possível quantidade
    MAX_QTD_CELL_LEN = 20

    # Tamanho máximo para fazer uma busca agressiva de números na célula
    MAX_QTD_STRICT_LEN = 10

    # =========================================================================
    # MOTOR DE EXTRAÇÃO
    # =========================================================================

    def parse(
        self, data: list[list[str]], state: ExtractionState, table_index: int
    ) -> list[ItemLicitacao]:
        """
        Método principal do parser DOCX. Varre a tabela linha a linha, identifica possíveis lotes,
        mapeamentos de cabeçalho e tenta extrair os itens da licitação.

        Args:
            data (list[list[str]]): A tabela extraída.
            state (ExtractionState): Objeto de estado da extração para manter contexto entre linhas.
            table_index (int): Índice da tabela sendo processada.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos da tabela.
        """
        items = []
        current_lote = None

        for row in data:
            # Verifica se a linha é apenas cabeçalho de lote
            lote_found = self._extract_lote_from_row(row)

            if lote_found:
                current_lote = lote_found
                logger.debug(f"Lote detectado na tabela {table_index}: {current_lote}")
                state["last_lote"] = current_lote
                continue

            # Tenta achar cabeçalho
            candidate_map = self._identify_columns(row)
            if candidate_map:
                current_map = state["current_header_map"]

                if not (current_map and self._is_repeated_header(row)):
                    state["current_header_map"] = candidate_map
                    logger.debug(f"Novo Header DOCX adotado: {candidate_map}")
                continue

            # Se já existe um cabeçalho, a linha deve ser um item
            if state["current_header_map"]:
                item = self._create_item_from_row(
                    row, state["current_header_map"], state, current_lote
                )
                if item:
                    items.append(item)

        return items

    def _create_item_from_row(
        self,
        row: list[str],
        header_map: dict[str, int],
        state: ExtractionState,
        current_lote: str | None,
    ) -> ItemLicitacao | None:
        """
        Orquestra a criação de um único `ItemLicitacao` a partir de uma linha de dados, gerenciando
        o contexto temporário e validando regras de negócio básicas antes de confirmar o item.

        Args:
            row (list[str]): Linha atual da tabela.
            header_map (dict[str, int]): Mapeamento das colunas.
            state (ExtractionState): Estado global da extração.
            current_lote (str | None): O lote atual (se algum).

        Returns:
            ItemLicitacao | None: O item formatado ou None se a linha for lixo.
        """
        active_lote = current_lote if current_lote else state["last_lote"]
        temp_state = state.copy()
        temp_state["last_lote"] = active_lote

        item = self._parse_row_docx(row, header_map, temp_state)

        if not item:
            return None

        desc_lower = item.objeto.lower()

        # Filtro de lixo
        if desc_lower.startswith(INVALID_DESC_PREFIXES):
            return None

        if not item.lote and active_lote:
            item.lote = active_lote

        # Atualiza o contador global de itens
        state["item_counter"] = temp_state["item_counter"]
        return item

    def _parse_row_docx(
        self, row: Sequence[str | None], mapping: dict[str, int], state: ExtractionState
    ) -> ItemLicitacao | None:
        """
        Extrai os campos individuais das linhas baseando-se no mapeamento.

        Args:
            row (Sequence[str | None]): Linha de dados.
            mapping (dict[str, int]): Mapeamento de colunas.
            state (ExtractionState): Estado para rastrear contadores.

        Returns:
            ItemLicitacao | None: Instância populada ou None em caso de falha silenciosa.
        """
        try:
            desc = self._extract_descricao(row, mapping)
            if not desc:
                return None

            qtd = self._extract_quantidade(row, state["item_counter"])
            if qtd is None:
                return None

            idx_item = mapping.get("item")
            final_item = self._update_item_counter(row, idx_item, state)
            final_lote = self._update_lote_state(row, mapping.get("lote"), state)
            unid = self._extract_unidade_fornecimento(row, mapping)

            return ItemLicitacao(
                item=final_item,
                quantidade=qtd,
                objeto=desc,
                unidade_fornecimento=unid,
                lote=final_lote,
            )

        except Exception as e:
            row_sample = str(row)[:60] + "..." if row else "Linha Vazia"
            logger.debug(
                f"Erro inesperado no parser DOCX. Linha ({row_sample}): {e}", exc_info=True
            )
            return None

    def _identify_columns(self, row: Sequence[str | None]) -> dict[str, int] | None:
        """
        Tenta descobrir a posição das colunas importantes na linha.
        Avalia sinônimos definidos em `HEADER_MAPPING` para "adivinhar" o cabeçalho.

        Args:
            row (Sequence[str | None]): Linha candidata a cabeçalho.

        Returns:
            dict[str, int] | None: Dicionário `{nome_da_coluna: índice}` ou None se não for
            cabeçalho.
        """
        row_str_raw = [str(c).strip() for c in row if c]

        # Se tem células gigantescas, muito provavelmente não é um cabeçalho
        if any(len(c) > self.MAX_HEADER_CELL_LENGTH for c in row_str_raw):
            return None

        mapping = {}
        row_str = [unidecode(str(c).lower().strip()) if c else "" for c in row]

        for key, synonyms in HEADER_MAPPING.items():
            for i, cell in enumerate(row_str):
                # Evita confundir coluna de "Valor Total" com "Quantidade"
                if self._should_skip_column_match(key, cell):
                    continue

                if self._match_synonym(synonyms, cell):
                    mapping[key] = i
                    break

            if key in mapping:
                continue

        # Só é considerado um cabeçalho válido se achou a descrição e atingiu a cota mínima de match
        if "objeto" in mapping and len(mapping) >= self.MIN_COLUMNS_FOR_HEADER:
            return mapping

        return None

    def _should_skip_column_match(self, key: str, cell: str) -> bool:
        """
        Impede que o parser cometa erros crassos de interpretação em colunas sensíveis, como ler
        uma coluna financeira ("Valor Unitário") achando que é a quantidade.

        Args:
            key (str): Chave interna do mapeamento.
            cell (str): Conteúdo da célula sendo avaliada.

        Returns:
            bool: True se deve ignorar esta célula para esta chave, False caso contrário.
        """
        return key in self.RESTRICTED_COLUMN_KEYS and any(
            v in cell for v in self.FORBIDDEN_COLUMN_TERMS
        )

    def _match_synonym(self, synonyms: Sequence[str], cell: str) -> bool:
        """
        Verifica se a célula contém algum dos sinônimos esperados.
        A busca procura por palavras completas ou exatas para evitar matches parciais errados.

        Args:
            synonyms (Sequence[str]): Sinônimos aceitáveis.
            cell (str): Conteúdo limpo da célula.

        Returns:
            bool: True se houve match.
        """
        for syn in synonyms:
            if syn == cell:
                return True

            if syn in cell:
                idx = cell.find(syn)
                prev_char = cell[idx - 1] if idx > 0 else " "
                next_char = cell[idx + len(syn)] if (idx + len(syn)) < len(cell) else " "

                if not prev_char.isalpha() and not next_char.isalpha():
                    return True

        return False

    def _is_repeated_header(self, row: Sequence[str | None]) -> bool:
        """
        Verifica se a linha atual é apenas uma reiteração de um cabeçalho.
        Usa busca por palavras-chave na linha inteira.

        Args:
            row (Sequence[str | None]): Linha atual.

        Returns:
            bool: True se parece ser um cabeçalho.
        """
        matches = 0
        row_str = " ".join(str(c).lower() for c in row if c)

        for keyword_group in self.HEADER_KEYWORD_GROUPS:
            if any(keyword in row_str for keyword in keyword_group):
                matches += 1

        return matches >= self.HEADER_MATCH_THRESHOLD

    def _extract_lote_from_row(self, row: Sequence[str | None]) -> str | None:
        """
        Procura proativamente por declarações de Lote ou Grupo na linha.

        Args:
            row (Sequence[str | None]): Linha de dados.

        Returns:
            str | None: O número/nome do lote se encontrado, None se não.
        """
        regex_lote = r"^\s*(?:LOTE|GRUPO)\b(?:\s*N[º°]?)?\s*[:|-]?\s*(\d+)"

        for cell in row:
            if not cell:
                continue
            cell_upper = str(cell).upper().strip()

            # Evita armadilhas
            if any(x in cell_upper for x in LOTE_BLOCK_WORDS):
                continue

            # Se encontrar o lote ou o grupo, os retorna
            if "LOTE" in cell_upper or "GRUPO" in cell_upper:
                match_cell = re.search(regex_lote, cell_upper)

                if match_cell:
                    return match_cell.group(1)

        # Fallback para procurar na linha inteira concatenada
        full_text = " ".join(str(c) for c in row if c).upper()
        if not any(x in full_text for x in LOTE_BLOCK_WORDS):
            match = re.search(regex_lote, full_text)

            if match:
                return match.group(1)

        return None

    def _update_lote_state(
        self, row: Sequence[str | None], idx_lote: int | None, state: ExtractionState
    ) -> str | None:
        """
        Atualiza e normaliza o lote em andamento, extraindo apenas os dígitos.

        Args:
            row (Sequence[str | None]): Linha de dados.
            idx_lote (int | None): Índice mapeado para a coluna de lote.
            state (ExtractionState): Estado global de extração.

        Returns:
            str | None: O lote normalizado.
        """
        lote_raw = get_text_safe(row, idx_lote)
        if lote_raw:
            lote_match = re.search(r"\d+", lote_raw)

            if lote_match:
                state["last_lote"] = normalize_lote(lote_match.group())

            else:
                state["last_lote"] = lote_raw

        return state["last_lote"]

    def _update_item_counter(
        self, row: Sequence[str | None], idx_item: int | None, state: ExtractionState
    ) -> int:
        """
        Gerencia o contador sequencial de itens.
        Se a coluna do DOCX estiver vazia, ele auto-incrementa o contador.
        Se houver salto absurdo de numeração, ele confia no sequencial em vez do valor sujo do DOCX.

        Args:
            row (Sequence[str | None]): Linha de dados.
            idx_item (int | None): Índice mapeado para a coluna de itens.
            state (ExtractionState): Estado global.

        Returns:
            int: O número final e validado do item para este registro.
        """
        item_str = get_text_safe(row, idx_item)
        item_raw = clean_number(item_str)
        if item_raw and item_raw > 0:
            final_item = int(item_raw)

            if final_item > state["item_counter"] + 10:
                final_item = state["item_counter"]

            state["item_counter"] = final_item + 1

        else:
            final_item = state["item_counter"]
            state["item_counter"] += 1

        return final_item

    def _extract_descricao(self, row: Sequence[str | None], mapping: dict[str, int]) -> str | None:
        """
        Tenta extrair o objeto (descrição do produto/serviço).
        Se a célula designada estiver vazia, procura a célula mais verbosa da linha inteira,
        assumindo que ali está a descrição que o DOCX deslocou.

        Args:
            row (Sequence[str | None]): Linha de dados.
            mapping (dict[str, int]): Mapeamento atual.

        Returns:
            str | None: O texto de descrição ou None se nada for suficientemente grande.
        """
        idx_desc = mapping.get("objeto")
        desc = get_text_safe(row, idx_desc)

        if not desc or len(desc) < self.MIN_DESC_FALLBACK_LEN:
            maior_texto = ""
            for cell in row:
                if cell and len(str(cell)) > len(maior_texto):
                    maior_texto = str(cell)

            if len(maior_texto) > self.MIN_DESC_FALLBACK_LEN:
                desc = maior_texto

        if not desc or len(desc) < MIN_DESC_LEN:
            return None

        return desc

    def _extract_quantidade(self, row: Sequence[str | None], expected_item: int) -> int | None:
        """
        Caça pela quantidade na linha inteira.

        Args:
            row (Sequence[str | None]): Linha de dados.
            expected_item (int): Número do item que estamos esperando (para não confundir).

        Returns:
            int | None: O número identificado como quantidade.
        """
        found_nums = []

        for cell in row:
            cstr = str(cell).strip() if cell else ""

            # Filtro para strings que possuem números mas não são quantidades
            if (
                len(cstr) > self.MAX_QTD_CELL_LEN
                or re.search(r"\d{2}/\d{2}/\d{4}", cstr)
                or "R$" in cstr.upper()
                or "ANEXO" in cstr.upper()
            ):
                continue

            # Tenta buscar números sozinhos na célula
            nums = re.findall(r"^\s*(\d+)\s*$", cstr.replace(".", ""))

            if not nums and len(cstr) <= self.MAX_QTD_STRICT_LEN:
                nums = re.findall(r"\d+", cstr.replace(".", ""))

            if nums:
                num_val = int(nums[0])
                if num_val > 0:
                    found_nums.append(num_val)

        if not found_nums:
            return None

        # Se o parser achou um número e esse número é igual ao do item atual, o remove
        if len(found_nums) > 1 and expected_item in found_nums:
            found_nums.remove(expected_item)

        qtd = found_nums[0]
        return qtd if qtd > 0 else None

    def _extract_unidade_fornecimento(
        self, row: Sequence[str | None], mapping: dict[str, int]
    ) -> str:
        """
        Busca a unidade de fornecimento.
        Caso o mapeamento não ache, varre as células contra um set predefinido de unidades válidas.

        Args:
            row (Sequence[str | None]): Linha de dados.
            mapping (dict[str, int]): Mapeamento atual.

        Returns:
            str: O nome da unidade ou um fallback "Unidade" se não encontrar.
        """
        idx_unid = mapping.get("unidade_fornecimento")
        unid_raw = get_text_safe(row, idx_unid)
        unid = clean_unidade_fornecimento(unid_raw)

        if unid.upper() == "UNIDADE":
            for cell in row:
                if cell:
                    cstr = str(cell).strip().upper()
                    if cstr in VALID_UNIDS_SET:
                        return cstr.capitalize()

        return unid
