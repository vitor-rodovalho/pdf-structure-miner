import logging
import re
from collections.abc import Sequence
from typing import TypedDict

from unidecode import unidecode

from src.core import (
    FINANCIAL_TOTALS_KEYS,
    GARBAGE_DESC_WORDS,
    HEADER_MAPPING,
    INVALID_DESC_PREFIXES,
    LOTE_BLOCK_WORDS,
    MAX_DESC_LEN,
    MAX_VALID_QUANTITY,
    MIN_DESC_LEN,
    MIN_DESC_NUMBER_LEN,
    TRASH_KEYWORDS,
)
from src.schemas import ItemLicitacao
from src.utils import (
    MAX_UNID_LEN,
    clean_number,
    clean_rows,
    clean_unidade_fornecimento,
    get_text_safe,
    normalize_lote,
)

logger = logging.getLogger(__name__)


class ExtractionState(TypedDict):
    last_lote: str | None
    item_counter: int
    current_header_map: dict[str, int] | None
    pending_broken_desc: str | None
    last_extracted_item: ItemLicitacao | None
    pending_item_num: int | None


class PDFTableParser:
    """
    Motor especializado em interpretar e extrair dados de matrizes/tabelas.
    Aplica heurísticas para detecção de cabeçalhos, lotes, e correção visual de colunas.
    """

    # =========================================================================
    # REGRAS DE ESTRUTURA E CABEÇALHO
    # =========================================================================

    # Quantidade mínima de colunas identificadas para confirmar que a linha é um cabeçalho
    HEADER_MATCH_THRESHOLD = 2

    # Mínimo de colunas validadas necessárias para inicializar o mapeamento do extrator
    MIN_COLUMNS_FOR_HEADER = 2

    # Tamanho mínimo da palavra na célula para aceitar um "match parcial" com o nome da coluna
    MIN_MATCH_LEN = 2

    # Colunas sensíveis que exigem validação rigorosa para não sofrerem falsos positivos
    RESTRICTED_COLUMN_KEYS = frozenset({"item", "quantidade"})

    # Termos (geralmente financeiros ou de unidade) que anulam a detecção das colunas restritas
    FORBIDDEN_COLUMN_TERMS = ("vlr", "valor", "preco", "preço", "total", "r$", "unit")

    # Limites para detecção de fallback do número do item
    MAX_ITEM_NUM_DIGITS = 4
    MAX_RAW_ITEM_LENGTH = 7

    # Tamanho máximo de uma string de quantidade
    MAX_QTD_STRING_LEN = 25

    # Tamanho máximo de uma célula para ela não ser mascarada ("cegada") no resgate lateral
    MAX_QTD_CELL_LEN = 20

    # Quantidade de partes esperadas ao dividir um número decimal/financeiro pela vírgula
    EXPECTED_DECIMAL_PARTS = 2

    # =========================================================================
    # MÉTODOS PRINCIPAIS E AUXILIARES
    # =========================================================================

    def parse_table(
        self,
        rows: Sequence[Sequence[str | None]],
        state: ExtractionState,
    ) -> list[ItemLicitacao]:
        """
        Processa as linhas de uma tabela extraída do PDF, identificando lotes, cabeçalhos e itens.

        Aplica heurísticas para detectar declarações de lote, mapeamento de colunas e extração de
        itens individuais, mantendo estado entre linhas para tratamento de campos distribuídos
        em múltiplas linhas.

        Args:
            rows (list[list[str | None]]): Linhas da tabela extraída do PDF.
            state (ExtractionState): Estado compartilhado entre linhas da tabela.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos da tabela.
        """
        items = []
        current_lote = None
        cleaned_rows = clean_rows(rows)

        for row in cleaned_rows:
            # Se a linha for um lote, atualiza o estado e pula para a próxima linha
            lote = self._extract_lote_from_row(row)
            if lote:
                norm_lote = normalize_lote(lote)
                current_lote = norm_lote
                state["last_lote"] = norm_lote
                continue

            # Se a linha for um cabeçalho, atualiza o estado e pula para a próxima linha
            header = self._identify_columns(row)
            if header and "quantidade" in header and "objeto" in header:
                state["current_header_map"] = header
                continue

            # Se a linha não for lote nem cabeçalho, tenta extrair um item com a função de parsing
            if state["current_header_map"]:
                item = self._create_item_from_row(
                    row, state["current_header_map"], state, current_lote
                )
                if item:
                    items.append(item)

        return items

    def _create_item_from_row(
        self,
        row: Sequence[str | None],
        header_map: dict[str, int],
        state: ExtractionState,
        current_lote: str | None,
    ) -> ItemLicitacao | None:
        """
        Cria um objeto ItemLicitacao a partir de uma linha de tabela usando a função de parsing
        fornecida.

        Args:
            row (Sequence[str | None]): Linha de tabela a ser processada.
            header_map (dict[str, int]): Mapeamento de colunas identificado para a tabela.
            state (ExtractionState): Estado compartilhado para controle de lotes e itens pendentes.
            current_lote (str | None): Lote atual identificado para associar ao item.

        Returns:
            ItemLicitacao | None: Objeto ItemLicitacao extraído ou None se a linha for considerada
            inválida ou lixo.
        """
        row_str = " ".join([str(c) for c in row if c]).lower()
        row_str_no_space = row_str.replace(" ", "")

        # Vasculha a linha atual para ver se ela anuncia um número de item
        for cell in row:
            if cell:
                # Captura padrões como "ITEM 01", "Item: 2", "ITEM N° 10" no início da célula
                match = re.search(r"(?i)^ITEM\s*(?:N[°º]\s*|:\s*)?(\d+)", str(cell).strip())
                if match:
                    state["pending_item_num"] = int(match.group(1))
                    break

        # Verifica se a linha contém lixo (termos financeiros ou rodapés/sensíveis)
        is_financial_trash = any(key in row_str_no_space for key in FINANCIAL_TOTALS_KEYS)
        is_general_trash = any(kw in row_str for kw in TRASH_KEYWORDS)

        if is_financial_trash or is_general_trash:
            state["pending_broken_desc"] = None
            state["last_extracted_item"] = None
            return None

        # Determina o lote ativo para associar ao item
        active = current_lote if current_lote else state.get("last_lote")
        if active:
            active = normalize_lote(active)
        state["last_lote"] = active

        item = self._parse_row(row, header_map, state)

        # Regras de limpeza final para a unidade de fornecimento e descrição
        if item:
            unid_str = str(item.unidade_fornecimento).strip().upper()

            if (
                not any(c.isalpha() for c in unid_str)
                or set(unid_str) == {"X"}
                or item.objeto.lower().strip().startswith(INVALID_DESC_PREFIXES)
                or len(item.objeto) < MIN_DESC_LEN
            ):
                return None

            if not item.lote and active:
                item.lote = active

        return item

    def _parse_row(
        self, row: Sequence[str | None], mapping: dict[str, int], state: ExtractionState
    ) -> ItemLicitacao | None:
        """
        Realiza o parsing de uma linha de tabela usando um mapeamento de colunas específico.

        Args:
            row: Linha de tabela a ser processada.
            mapping: Mapeamento de colunas identificadas para a tabela.
            state: Estado compartilhado para controle de lotes e itens pendentes.

        Returns:
            ItemLicitacao | None: Objeto ItemLicitacao extraído ou None se a linha for considerada
            inválida ou lixo.
        """
        try:
            if not mapping:
                return None

            # Extrações Iniciais
            raw_desc = get_text_safe(row, mapping.get("objeto"))
            val_qtd = get_text_safe(row, mapping.get("quantidade"))

            raw_item = clean_number(get_text_safe(row, mapping.get("item")))
            try:
                item_num = int(float(raw_item)) if raw_item is not None else None
            except (ValueError, TypeError):
                item_num = None

            # Se o mapa falhou, mas a primeira coluna é um número isolado, é o Item
            if item_num is None:
                cand_item = str(get_text_safe(row, 0) or "").strip()
                clean_cand_item = re.sub(r"[^\d]", "", cand_item)
                if (
                    clean_cand_item
                    and len(clean_cand_item) <= self.MAX_ITEM_NUM_DIGITS
                    and len(cand_item) <= self.MAX_RAW_ITEM_LENGTH
                ):
                    item_num = int(clean_cand_item)

            qtd = self._extract_quantidade_table(row, val_qtd, mapping.get("quantidade"))
            has_valid_desc = bool(raw_desc and len(raw_desc) >= MIN_DESC_LEN)

            # Se a descrição tem palavras que indicam lixo, ignora a linha inteira e reseta buffers
            if has_valid_desc and any(w in raw_desc.upper() for w in GARBAGE_DESC_WORDS):
                state["last_extracted_item"] = None
                state["pending_broken_desc"] = None
                return None

            # Resolve a fragmentação de linhas
            final_desc = self._resolve_row_buffer(state, has_valid_desc, qtd, item_num, raw_desc)

            if not qtd or not final_desc or len(final_desc) < MIN_DESC_LEN:
                return None

            # Remove lixo de "R$" vazio que possa ter vazado das colunas de valores
            final_desc = re.sub(r"(?i)\bR\$\s*R\$?\b", "", final_desc).strip()
            final_desc = re.sub(r"(?i)\bR\$\b", "", final_desc).strip()

            if clean_number(final_desc) is not None and len(final_desc) < MIN_DESC_NUMBER_LEN:
                return None

            # Criação do Objeto
            final_item = self._update_item_counter(row, mapping.get("item"), state)
            final_lote = self._update_lote_state(row, mapping.get("lote"), state)
            unid = self._extract_unidade_table(row, val_qtd, mapping.get("unidade_fornecimento"))

            # Se usar o item flutuante, força-o como o número oficial e sincroniza o contador
            pending_num = state.get("pending_item_num")
            if pending_num is not None:
                safe_pending_num = int(pending_num)
                final_item = safe_pending_num
                state["item_counter"] = safe_pending_num + 1
                state["pending_item_num"] = None

            novo_item = ItemLicitacao(
                item=final_item,
                quantidade=qtd,
                objeto=final_desc,
                unidade_fornecimento=unid,
                lote=final_lote,
            )

            # Guarda referência para caso a próxima linha seja de continuação
            state["last_extracted_item"] = novo_item

            return novo_item

        except Exception as e:
            row_sample = str(row)[:60] + "..." if row else "Linha Vazia"
            logger.debug(
                f"Falha no parser de tabelas (PDF). Linha ({row_sample}): {e}", exc_info=True
            )
            return None

    def _identify_columns(self, row: Sequence[str | None]) -> dict[str, int] | None:
        """
        Varre uma linha da tabela buscando palavras-chave que definam os títulos das colunas
        (Item, Objeto, Quantidade, Unidade).

        Args:
            row (Sequence[str | None]): Linha de texto higienizada.

        Returns:
            dict[str, int] | None: Mapeamento {nome_da_coluna: indice}, ou None se inválido.
        """
        mapping = {}
        row_str = [unidecode(str(c).lower().strip()) if c else "" for c in row]

        # Busca o mapping do arquivo de configuração
        header_map = HEADER_MAPPING

        for key, synonyms in header_map.items():
            for i, cell in enumerate(row_str):
                if self._should_skip_column_match(key, cell):
                    continue

                if self._match_synonym(synonyms, cell):
                    mapping[key] = i
                    break

            # Se já achou a coluna para esta chave, pula para a próxima chave
            if key in mapping:
                continue

        # Para ser considerado um cabeçalho viável, a tabela precisa ter obrigatoriamente a coluna
        # "objeto" e pelo menos mais uma coluna auxiliar
        if "objeto" in mapping and len(mapping) >= self.MIN_COLUMNS_FOR_HEADER:
            return mapping

        return None

    def _should_skip_column_match(self, key: str, cell: str) -> bool:
        """
        Evita o falso positivo de confundir colunas financeiras com colunas de quantidade/item.

        Args:
            key (str): O nome da coluna que está sendo avaliada (ex: "item", "quantidade").
            cell (str): O conteúdo da célula que está sendo verificada.

        Returns:
            bool: True se a correspondência deve ser ignorada, False caso contrário.
        """
        return key in self.RESTRICTED_COLUMN_KEYS and any(
            v in cell for v in self.FORBIDDEN_COLUMN_TERMS
        )

    def _match_synonym(self, synonyms: Sequence[str], cell: str) -> bool:
        """
        Verifica se algum dos sinônimos está presente na célula de forma isolada.

        Args:
            synonyms (Sequence[str]): Lista de sinônimos a serem verificados.
            cell (str): O conteúdo da célula a ser verificada.

        Returns:
            bool: True se encontrar um sinônimo isolado, False caso contrário.
        """
        for syn in synonyms:
            if syn == cell:
                return True

            if syn in cell:
                idx = cell.find(syn)
                prev_char = cell[idx - 1] if idx > 0 else " "
                next_char = cell[idx + len(syn)] if (idx + len(syn)) < len(cell) else " "

                # Só considera match se o sinônimo não estiver engolido no meio de outra palavra
                if not prev_char.isalpha() and not next_char.isalpha():
                    return True
        return False

    def _is_repeated_header(self, row: Sequence[str | None], header_map: dict[str, int]) -> bool:
        """
        Verifica se a linha atual é uma repetição do cabeçalho da tabela, baseando-se na
        correspondência estrita dos índices de coluna mapeados.

        Args:
            row (Sequence[str | None]): Linha de dados atual.
            header_map (dict[str, int]): Mapeamento atual de colunas.

        Returns:
            bool: True se encontrar correspondências suficientes para ser considerado cabeçalho.
        """
        test_cols = [k for k in header_map if k in ["item", "objeto", "quantidade"]]
        matches = 0

        for col_name in test_cols:
            idx = header_map[col_name]

            if idx < len(row):
                cell_raw = row[idx]
                if cell_raw is not None:
                    cell_val = unidecode(str(cell_raw).lower())

                    # Correspondência exata ou match parcial se a string for maior que o mínimo
                    if col_name in cell_val or (
                        len(cell_val) > self.MIN_MATCH_LEN and cell_val in col_name
                    ):
                        matches += 1

        return matches >= self.HEADER_MATCH_THRESHOLD

    def _extract_lote_from_row(self, row: Sequence[str | None]) -> str | None:
        """
        Verifica se a linha contém uma declaração de Lote ou Grupo.
        Possui blindagem contra falsos positivos comuns, como endereços contendo a palavra "Lote".

        Args:
            row (Sequence[str | None]): Linha higienizada de dados da tabela.

        Returns:
            str | None: O número/código do Lote encontrado ou None.
        """
        # Captura "LOTE", "LOTE N°", "GRUPO 1", etc., ignorando espaços extras
        regex_lote = r"^\s*(?:LOTE|GRUPO)\b(?:\s*N[º°]?)?\s*[:|-]?\s*(\d+)"

        # Procura célula por célula
        for cell in row:
            if not cell:
                continue

            cell_upper = cell.upper().strip()

            # Se a célula tiver palavras de endereço, aborta a busca nesta célula
            if any(x in cell_upper for x in LOTE_BLOCK_WORDS):
                continue

            if "LOTE" in cell_upper or "GRUPO" in cell_upper:
                match_cell = re.search(regex_lote, cell_upper)
                if match_cell:
                    return match_cell.group(1)

        # Tenta encontrar na linha inteira concatenada
        # Usa compreensão de lista para garantir que 'None' não seja concatenado
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
        Extrai o lote da coluna específica e atualiza a memória de lote global (state).

        Args:
            row (Sequence[str | None]): Linha de dados atual.
            idx_lote (int | None): Índice da coluna de lote/grupo.
            state (ExtractionState): Estado global da extração.

        Returns:
            str | None: O lote normalizado encontrado ou o lote salvo no estado.
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
        Atualiza e retorna o contador sequencial de itens.
        Implementa proteção contra saltos discrepantes causados por lixo numérico.

        Args:
            row (Sequence[str | None]): Linha de dados atual.
            idx_item (int | None): Índice mapeado da coluna de itens.
            state (ExtractionState): Dicionário de estado contendo o 'item_counter'.

        Returns:
            int: O número consolidado do item atual.
        """
        item_str = get_text_safe(row, idx_item)
        item_raw = clean_number(item_str)

        if item_raw and item_raw > 0:
            final_item = int(item_raw)

            # Heurística de salto: Se o ID saltou mais de 10 posições de uma vez, assume que o
            # extrator leu um código solto em vez do ID do item
            if final_item > state["item_counter"] + 10:
                final_item = state["item_counter"]

            state["item_counter"] = final_item + 1

        # Caso não ache um número válido, incrementa a partir da memória
        else:
            final_item = state["item_counter"]
            state["item_counter"] += 1

        return final_item

    def _resolve_row_buffer(
        self,
        state: ExtractionState,
        has_valid_desc: bool,
        qtd: int | None,
        item_num: int | None,
        raw_desc: str,
    ) -> str | None:
        """
        Gerencia o buffer para costurar linhas que foram quebradas no PDF.

        Args:
            state (ExtractionState): Dicionário que guarda o estado atual da extração.
            has_valid_desc (bool): Indicador que confirma se a linha atual possui um
            texto de descrição válido.
            qtd (int | None): A quantidade numérica do produto encontrada na linha atual.
            item_num (int | None): Número do item extraído da linha atual.
            raw_desc (str): O texto bruto da coluna de descrição extraído da linha atual.

        Returns:
            str | None: Retorna a string da descrição completa se as condições indicarem
            que o item está finalizado ou None caso contrário.
        """
        pending_desc = state.get("pending_broken_desc")
        final_desc = None

        # Se a descrição é válida, mas não tem quantidade nem número de item, pode ser uma
        # linha de continuação da descrição
        if has_valid_desc and not qtd and not item_num:
            last_item = state.get("last_extracted_item")
            if last_item and len(last_item.objeto) < MAX_DESC_LEN:
                last_item.objeto += " " + raw_desc

        # Se a descrição é válida e tem número de item, mas não tem quantidade, pode ser início
        # de uma descrição quebrada. Guarda no buffer para tentar completar na próxima linha
        elif has_valid_desc and not qtd and item_num:
            state["pending_broken_desc"] = raw_desc
            state["last_extracted_item"] = None

        # Se a descrição não é válida, mas tem quantidade, e tem uma descrição pendente no
        # buffer, é o fim da descrição quebrada. Junta e limpa o buffer
        elif not has_valid_desc and qtd and pending_desc:
            final_desc = pending_desc
            state["pending_broken_desc"] = None

        # Se a descrição é válida e tem quantidade, é um caso normal
        elif has_valid_desc and qtd:
            final_desc = (pending_desc + " " + raw_desc) if pending_desc else raw_desc
            state["pending_broken_desc"] = None

        return final_desc

    def _extract_quantidade_table(
        self, row: Sequence[str | None], val_qtd: str, idx_qtd: int | None
    ) -> int | None:
        """
        Limpa a string de quantidade e tenta resgate lateral se falhar.

        Args:
            row (Sequence[str | None]): Linha de dados atual.
            val_qtd (str): Valor bruto extraído da coluna de quantidade.
            idx_qtd (int | None): Índice da coluna de quantidade.

        Returns:
            int | None: Quantidade numérica encontrada ou None.
        """
        qtd = None

        if val_qtd:
            val_limpo = val_qtd.strip().upper()

            # Trata valores financeiros
            if not (val_limpo.startswith("R$") or val_limpo.startswith("RS")):
                val_sem_moeda = re.sub(r"R\$\s*[\d\.,]+", "", val_limpo)
                val_sem_moeda = re.sub(r",\d{2}\b", "", val_sem_moeda)

                if len(val_sem_moeda) < self.MAX_QTD_STRING_LEN:
                    nums = re.findall(r"\d+", val_sem_moeda.replace(".", ""))
                    if nums:
                        qtd = int(nums[0])

        if not qtd:
            safe_row = []
            for c in row:
                cstr = str(c).strip() if c else ""
                # Mascara células perigosas (datas, dinheiros, anexos) para a heurística
                if (
                    len(cstr) > self.MAX_QTD_CELL_LEN
                    or re.search(r"\d{2}/\d{2}/\d{4}", cstr)
                    or "ANEXO" in cstr.upper()
                    or ("R$" in cstr.upper())
                    or (
                        "," in cstr
                        and len(cstr.split(",")) == self.EXPECTED_DECIMAL_PARTS
                        and cstr.split(",")[1][:2].isdigit()
                    )
                ):
                    safe_row.append("XXX")
                else:
                    safe_row.append(c)
            qtd = self._extract_quantidade_heuristic(safe_row, idx_qtd)

        return qtd

    def _extract_quantidade_heuristic(
        self, row: Sequence[str | None], idx_qtd: int | None
    ) -> int | None:
        """
        Tenta encontrar a quantidade nas colunas vizinhas caso a coluna mapeada falhe ou tenha
        sofrido deslocamento.

        Args:
            row (Sequence[str | None]): Linha de dados atual.
            idx_qtd (int | None): Índice original esperado para a quantidade.

        Returns:
            int | None: Quantidade numérica encontrada ou None.
        """
        if idx_qtd is None:
            return None

        # Ordem de tentativa: própria coluna, próxima, anterior
        indices_to_try = [idx_qtd, idx_qtd + 1, idx_qtd - 1]

        for i in indices_to_try:
            if 0 <= i < len(row):
                candidate_str = get_text_safe(row, i)
                if not candidate_str:
                    continue

                cand_strip = candidate_str.strip()
                clean_cand_qtd = re.sub(r"[^\d]", "", cand_strip)

                if (
                    i == 0
                    and idx_qtd != 0
                    and clean_cand_qtd
                    and len(clean_cand_qtd) <= self.MAX_ITEM_NUM_DIGITS
                    and len(cand_strip) <= self.MAX_RAW_ITEM_LENGTH
                ):
                    continue

                qtd_candidate = clean_number(candidate_str)

                if qtd_candidate:
                    return int(qtd_candidate)

        return None

    def _extract_unidade_table(
        self, row: Sequence[str | None], val_qtd: str, idx_unid: int | None
    ) -> str:
        """
        Extrai a unidade e aplica a regra de resgate da coluna de unidade de fornecimento.

        Args:
            row (Sequence[str | None]): Linha de dados atual.
            val_qtd (str): Valor bruto extraído da coluna de quantidade.
            idx_unid (int | None): Índice da coluna de unidade de fornecimento.

        Returns:
            str: Unidade de fornecimento extraída ou limpa.
        """
        unid_raw = None

        if idx_unid is not None:
            # Ordem de tentativa: própria coluna, próxima, anterior
            indices_to_try = [idx_unid, idx_unid + 1, idx_unid - 1]

            for i in indices_to_try:
                candidate = get_text_safe(row, i)

                # Verifica se o candidato tem texto e contém letras
                if candidate and any(c.isalpha() for c in candidate):
                    unid_raw = candidate
                    break

        unid = clean_unidade_fornecimento(unid_raw)

        if unid.upper() == "UNIDADE" and val_qtd:
            possible_unid = re.sub(r"[\d\.,]", "", val_qtd)
            possible_unid = re.sub(r"(?i)R\$|RS", "", possible_unid).strip()
            if (
                possible_unid
                and len(possible_unid) <= MAX_UNID_LEN
                and any(c.isalpha() for c in possible_unid)
            ):
                unid = clean_unidade_fornecimento(possible_unid)

        return unid

    def _recover_item_from_row(
        self, row: Sequence[str | None], state: ExtractionState, current_lote: str | None
    ) -> ItemLicitacao | None:
        """
        Tenta pescar dados que escorregaram das colunas originais.
        Atua como fallback quando a formatação da tabela está corrompida.

        Args:
            row (Sequence[str | None]): Linha de dados atual.
            state (ExtractionState): Estado global da extração.
            current_lote (str | None): Lote detectado na linha atual, se houver.

        Returns:
            ItemLicitacao | None: Item reconstruído ou None se não conseguir recuperar dados
            válidos.
        """
        try:
            clean_row = [str(val).replace("\n", " ").strip() if val else "" for val in row]

            # Resgate da Descrição
            desc = self._recover_descricao(clean_row)
            if not desc:
                return None

            # Filtra todas as células que contêm números viáveis
            possible_numbers = []
            for cell in clean_row:
                num = clean_number(cell)

                if num is not None:
                    possible_numbers.append((num, cell))

            if not possible_numbers:
                return None

            # Resgate da Quantidade e Unidade de Fornecimento
            qtd, unid = self._recover_quantidade_and_unidade_fornecimento(
                possible_numbers, state["item_counter"]
            )

            if qtd is None:
                return None

            # Consolidação do Item e Estado
            final_item = state["item_counter"]
            state["item_counter"] += 1

            # Busca secundária por uma unidade genérica se a principal falhou
            if unid == "Unidade":
                for cell in clean_row:
                    if 0 < len(cell) <= MAX_UNID_LEN and cell.isalpha():
                        unid = cell.upper()
                        break

            final_lote = current_lote if current_lote else state.get("last_lote")

            return ItemLicitacao(
                item=final_item,
                quantidade=qtd,
                objeto=desc,
                unidade_fornecimento=unid,
                lote=final_lote,
            )

        except Exception as e:
            # Registra um pedaço da linha que causou a falha
            row_sample = str(row)[:60] + "..." if row else "Linha Vazia"

            logger.debug(
                f"Falha na heurística de resgate (Fallback). Linha ({row_sample}): {e}",
                exc_info=True,
            )
            return None

    def _recover_descricao(self, clean_row: list[str]) -> str | None:
        """
        Encontra a descrição assumindo que é o texto mais longo da linha que não é classificado
        como um número.

        Args:
            clean_row (list[str]): Linha higienizada de dados da tabela.

        Returns:
            str | None: Descrição recuperada ou None se não encontrar uma descrição válida.
        """
        candidates_desc = []

        for cell in clean_row:
            # A descrição deve ter um comprimento mínimo e não deve ser confundida com um número
            if len(cell) > MIN_DESC_LEN and not clean_number(cell):
                candidates_desc.append((len(cell), cell))

        if not candidates_desc:
            return None

        candidates_desc.sort(key=lambda x: x[0], reverse=True)

        return candidates_desc[0][1]

    def _recover_quantidade_and_unidade_fornecimento(
        self, possible_numbers: list[tuple[float, str]], current_item_idx: int
    ) -> tuple[int | None, str]:
        """
        Analisa números encontrados na linha para deduzir a quantidade e unidade.

        Args:
            possible_numbers (list[tuple[float, str]]): Lista de tuplas contendo números encontrados
            e seus textos originais.
            current_item_idx (int): O número do item atual para evitar confusão com a quantidade.

        Returns:
            tuple[int | None, str]: Quantidade numérica encontrada (ou None) e a unidade de
            fornecimento deduzida (ou "Unidade" como padrão).
        """
        qtd = None
        unid = "Unidade"

        for num, original_cell in possible_numbers:
            val = int(num)

            # Pula o ID do item atual/anterior e números gigantes
            if val in (current_item_idx, current_item_idx - 1) or val > MAX_VALID_QUANTITY:
                continue

            qtd = val
            text_in_cell = re.sub(r"[\d\.,]", "", original_cell).strip()

            if text_in_cell and len(text_in_cell) <= MAX_UNID_LEN:
                unid = text_in_cell.upper()

            break

        # Se não encontrou nas regras acima, pega o último número disponível
        if qtd is None and possible_numbers:
            last_num, last_cell = possible_numbers[-1]
            qtd = int(last_num)
            text_in_cell = re.sub(r"[\d\.,]", "", last_cell).strip()

            if text_in_cell and len(text_in_cell) <= MAX_UNID_LEN:
                unid = text_in_cell.upper()

        return qtd, unid
