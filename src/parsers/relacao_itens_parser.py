import logging
import re

from src.schemas import ItemLicitacao
from src.utils import clean_number, deduplicate_items

logger = logging.getLogger(__name__)


class RelacaoItensParser:
    """
    Parser especializado em interpretar textos extraídos de PDFs no formato 'Relação de Itens'.
    """

    # =========================================================================
    # CONSTANTES ESPECÍFICAS DAS RELAÇÕES DE ITENS
    # =========================================================================

    # Padrão de campos intrusos que grudam nos valores no formato "Relação de Itens"
    INTRUDER_PATTERN = re.compile(
        r"critério de|menor preço|valor estimado|valor unitário|valor total|"
        r"unidade de fornecimento|quantidade m[íi]nima|quantidade m[áa]xima|"
        r"intervalo m[íi]nimo|local de entrega|grupo:",
        re.IGNORECASE,
    )

    # Regex para capturar DD/MM/AAAA HH:MM (padrão de rodapé de sistema)
    FOOTER_DATE_PATTERN = re.compile(r"\d{2}/\d{2}/\d{4}\s+\d{2}:\d{2}")

    # Termos financeiros ou de editais que o parser de texto deve pular imediatamente
    STOP_KEYS = frozenset(
        [
            "tratamento diferenciado",
            "aplicabilidade decreto",
            "quantidade mínima",
            "critério de julgamento",
            "menor preço",
            "critério de valor",
            "valor estimado",
            "valor unitário",
            "quantidade máxima",
            "intervalo mínimo",
            "local de entrega",
            "valor total",
        ]
    )

    # Palavras que indicam cabeçalhos/rodapés inúteis no meio da página
    RODAPE_WORDS = frozenset(["página", "uasg", "fls."])

    # =========================================================================
    # MOTOR DE INTERPRETAÇÃO
    # =========================================================================

    def parse(self, full_text: str) -> list[ItemLicitacao]:
        """
        Parser para documentos Relações de Itens oficiais.
        Orquestra a leitura linha a linha delegando o processamento aos helpers.

        Args:
            full_text (str): Texto completo do PDF extraído.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos do documento.
        """
        lines = self._trim_text(full_text)
        items = []
        curr_item = {}
        item_start_pattern = re.compile(r"^\s*(\d+)\s+-\s+(.+)$")

        for i, raw_line in enumerate(lines):
            line = raw_line.strip()
            if not line:
                continue

            # Detecta Início de um Novo Item
            match = item_start_pattern.match(line)
            if match:
                title = match.group(2).strip()
                if "itens da licitação" in title.lower():
                    continue

                self._save_item(curr_item, items)
                curr_item = {"item": int(match.group(1)), "objeto": title, "full_desc": []}
                continue

            # Se não inicializou o primeiro item ainda, ignora
            if not curr_item:
                continue

            lower_line = line.lower()

            # Tenta processar como um campo chave
            if self._process_fields(line, lower_line, lines, i, curr_item):
                continue

            # Se não for campo chave, é texto livre
            if "full_desc" in curr_item:
                if any(x in lower_line for x in self.RODAPE_WORDS):
                    continue
                if self.FOOTER_DATE_PATTERN.search(line):
                    continue

                curr_item["full_desc"].append(line)

        # Salva o último item processado
        self._save_item(curr_item, items)

        return deduplicate_items(items)

    def _trim_text(self, full_text: str) -> list[str]:
        """
        Corta o cabeçalho e o rodapé do documento e retorna as linhas limpas.

        Args:
            full_text (str): Texto completo do PDF extraído.

        Returns:
            list[str]: Linhas de texto relevantes para a extração dos itens.
        """
        marker_match = re.search(r"1\s*[-]\s*Itens\s+da\s+Licitação", full_text, re.IGNORECASE)
        if marker_match:
            full_text = full_text[marker_match.start() :]

        stop_match = re.search(r"2\s*[-]\s*Composição\s+dos\s+Grupos", full_text, re.IGNORECASE)
        if stop_match:
            full_text = full_text[: stop_match.start()]

        return full_text.split("\n")

    def _process_fields(
        self, line: str, lower: str, lines: list[str], idx: int, curr_item: dict
    ) -> bool:
        """
        Identifica qual campo a linha contém e delega a extração.

        Args:
            line (str): Linha de texto atual.
            lower (str): Linha de texto em minúsculas para facilitar a detecção.
            lines (list[str]): Lista de todas as linhas do documento.
            idx (int): Índice da linha atual na lista de linhas.
            curr_item (dict): Dicionário do item atual sendo construído.

        Returns:
            bool: True se a linha foi processada como um campo chave, False caso contrário.
        """
        if "quantidade total:" in lower:
            self._extract_quantidade(line, lines, idx, curr_item)
            return True

        if "unidade de fornecimento:" in lower:
            self._extract_unidade(line, lines, idx, curr_item)
            return True

        if "grupo:" in lower:
            self._extract_grupo(line, lines, idx, curr_item)
            return True

        if "descrição detalhada:" in lower:
            self._extract_desc(line, curr_item)
            return True

        return any(k in lower for k in self.STOP_KEYS)

    def _extract_quantidade(self, line: str, lines: list[str], idx: int, curr_item: dict) -> None:
        """
        Extrai a quantidade do item.

        Args:
            line (str): Linha de texto atual.
            lower (str): Linha de texto em minúsculas para facilitar a detecção.
            lines (list[str]): Lista de todas as linhas do documento.
            idx (int): Índice da linha atual na lista de linhas.
            curr_item (dict): Dicionário do item atual sendo construído.
        """
        clean_quantidade = clean_number(self._get_value(line, lines, idx))
        if clean_quantidade:
            curr_item["quantidade"] = int(clean_quantidade)

    def _extract_unidade(self, line: str, lines: list[str], idx: int, curr_item: dict) -> None:
        """
        Extrai e limpa a unidade de fornecimento.

        Args:
            line (str): Linha de texto atual.
            lines (list[str]): Lista de todas as linhas do documento.
            idx (int): Índice da linha atual na lista de linhas.
            curr_item (dict): Dicionário do item atual sendo construído.
        """
        val = self._get_value(line, lines, idx)
        if not val or val.strip().replace(".", "").replace(",", "").isdigit():
            return

        val = re.sub(r"^[\d\.,\s]+", "", val)
        curr_item["unidade_fornecimento"] = val.strip()

    def _extract_grupo(self, line: str, lines: list[str], idx: int, curr_item: dict) -> None:
        """
        Extrai o lote/grupo.

        Args:
            line (str): Linha de texto atual.
            lines (list[str]): Lista de todas as linhas do documento.
            idx (int): Índice da linha atual na lista de linhas.
            curr_item (dict): Dicionário do item atual sendo construído.
        """
        val = self._get_value(line, lines, idx)
        if val:
            curr_item["lote"] = re.sub(r"(?i)grupo\s*:?", "", val).strip()

    def _extract_desc(self, line: str, curr_item: dict) -> None:
        """
        Extrai a descrição detalhada do item.

        Args:
            line (str): Linha de texto atual.
            curr_item (dict): Dicionário do item atual sendo construído.
        """
        parts = line.split(":", 1)
        if len(parts) > 1 and parts[1].strip():
            curr_item["full_desc"].append(parts[1].strip())

    def _get_value(self, line: str, lines: list, idx: int) -> str:
        """
        Helper que fatia valores grudados gerados pela extração do texto PDF.

        Args:
            line (str): Linha de texto atual.
            lines (list[str]): Lista de todas as linhas do documento.
            idx (int): Índice da linha atual na lista de linhas.

        Returns:
            str: Valor extraído ou string vazia se não encontrar nada válido.
        """
        parts = line.split(":", 1)

        if len(parts) > 1:
            val_inline = parts[1].strip()
            val_inline = self.INTRUDER_PATTERN.split(val_inline)[0].strip()

            # Só retorna se sobrar um valor real. Se sobrou vazio, a busca continua
            if val_inline and val_inline != ":":
                return val_inline

        # Se o valor não está na mesma linha, desce procurando
        for offset in range(1, 9):
            if idx + offset < len(lines):
                candidate = lines[idx + offset].strip()
                if not candidate:
                    continue

                clean_candidate = self.INTRUDER_PATTERN.split(candidate)[0].strip()

                if clean_candidate and clean_candidate != ":":
                    # Se após limpar, ainda tem um ":" e não é uma data/hora, é outro campo
                    if ":" in clean_candidate and not self.FOOTER_DATE_PATTERN.search(candidate):
                        continue
                    return clean_candidate

        return ""

    def _save_item(self, curr: dict, items: list) -> None:
        """
        Salva o item atual construído na lista final de itens.

        Args:
            curr (dict): Dicionário do item atual sendo construído.
            items (list): Lista de itens extraídos até agora, onde o item finalizado é adicionado.
        """
        if "item" in curr:
            body_desc = " ".join(curr["full_desc"]).strip()

            if not body_desc:
                return

            items.append(
                ItemLicitacao(
                    item=curr["item"],
                    objeto=body_desc,
                    quantidade=curr.get("quantidade", 1),
                    unidade_fornecimento=curr.get("unidade_fornecimento", "Unidade"),
                    lote=curr.get("lote"),
                )
            )
        curr.clear()
