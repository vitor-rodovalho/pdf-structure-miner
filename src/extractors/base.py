import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import TypedDict

from unidecode import unidecode

from src.core.config import HEADER_MAPPING
from src.schemas.licitacao import ItemLicitacao

logger = logging.getLogger(__name__)


class ExtractionState(TypedDict):
    last_lote: str | None
    item_counter: int


class BaseExtractor(ABC):
    """
    Classe abstrata que define a interface obrigatória para todos os extratores.
    """

    @abstractmethod
    def extract(self, file_path: Path) -> list[ItemLicitacao]:
        """
        Processa o arquivo e retorna uma lista de itens estruturados.

        Args:
            file_path (Path): Caminho completo do arquivo.

        Returns:
            list[ItemLicitacao]: Lista de itens extraídos.
        """
        pass


    def _identify_columns(self, row: Sequence[str | None]) -> dict[str, int] | None:
        """
        Identifica quais colunas correspondem a quais campos.

        Args:
            row (Sequence[str | None]): Linha de texto extraída do PDF, geralmente o cabeçalho
            da tabela.

        Returns:
            dict[str, int] | None: Dicionário mapeando campos do esquema para índices
            de coluna, ou None se não conseguir identificar.
        """

        # Remove None, acentos e deixa minúsculo
        clean_row = [unidecode(str(c or "")).lower().strip() for c in row]
        mapping = {}

        for field, synonyms in HEADER_MAPPING.items():
            for idx, cell_text in enumerate(clean_row):
                # Verifica se alguma palavra-chave está na célula
                if any(syn in cell_text for syn in synonyms):
                    mapping[field] = idx
                    break

        # Só aceita se achou pelo menos Objeto e Quantidade
        if "objeto" in mapping and "quantidade" in mapping:
            logger.debug(f"Mapeamento encontrado: {mapping} | Header Original: {clean_row}")
            return mapping
        return None


    def _parse_row(
        self, row: Sequence[str | None], mapping: dict[str, int], state: ExtractionState
    ) -> ItemLicitacao | None:
        """
        Converte uma linha bruta em um objeto ItemLicitacao validado.

        Args:
            row (Sequence[str | None]): Linha de texto extraída do PDF representando um item.
            mapping (dict[str, int]): Dicionário mapeando campos do esquema para índices
                                    de coluna, identificado a partir do cabeçalho.
            state (ExtractionState): Estado atual da extração, mantendo informações como último
                                    lote e contador de itens.

        Returns:
            ItemLicitacao | None: Objeto representando o item, ou None se a linha for
                                inválida ou incompleta.

        Raises:
            Exception: Qualquer erro durante a conversão dos dados.
        """

        try:
            # Função auxiliar para pegar valor seguro da lista com base no mapeamento
            def get_val(field):
                """
                Retorna o valor de uma coluna mapeada, ou vazio se não encontrada.

                Args:
                    field (str): Nome do campo mapeado.

                Returns:
                    str: Valor da célula correspondente ao campo, ou string vazia.
                """
                idx = mapping.get(field)
                if idx is not None and idx < len(row):
                    val = row[idx]
                    return str(val).replace("\n", " ").strip() if val else ""
                return ""

            desc = get_val("objeto")
            qtd_str = get_val("quantidade")

            if not desc or not qtd_str:
                return None

            if not desc or not qtd_str:
                logger.debug(f"Linha incompleta ignorada. Desc: '{desc[:20]}...', Qtd: '{qtd_str}'")
                return None

            # Limpeza e conversão da quantidade para número
            qtd = self._clean_number(qtd_str)
            if not qtd:
                logger.debug(f"Quantidade inválida ignorada: '{qtd_str}' | Linha: {row}")
                return None

            item_raw = self._clean_number(get_val("item"))
            if item_raw and item_raw > 0:
                final_item = int(item_raw)
                # Sincroniza o contador se encontrar um número válido
                state["item_counter"] = final_item + 1

            else:
                # Fallback: Usa o contador sequencial
                final_item = state["item_counter"]
                # Incrementa para o próximo item
                state["item_counter"] += 1

            lote_raw = get_val("lote")
            if lote_raw:
                state["last_lote"] = lote_raw  # Atualiza memória

            # Se não tem lote na linha, usa o último visto
            final_lote = lote_raw if lote_raw else state["last_lote"]

            return ItemLicitacao(
                item=final_item,
                quantidade=int(qtd),
                objeto=desc,
                unidade_fornecimento=get_val("unidade_fornecimento") or "UN",
                lote=final_lote,
            )

        except Exception as e:
            logger.debug(f"Erro ao converter linha: {e} | Linha: {row}")
            return None


    @staticmethod
    def _clean_number(val: str) -> float | None:
        """
        Converte strings numéricas brasileiras para float/int.

        Args:
            val (str): String representando um número, possivelmente com formatação
            brasileira (ex: "1.234,56").

        Returns:
            float | None: Valor numérico convertido, ou None se não for possível
            converter.

        Raises:
            Exception: Qualquer erro durante a limpeza ou conversão do número.
        """

        try:
            # Remove pontos de milhar e troca vírgula por ponto
            clean = val.replace(".", "").replace(",", ".")

            # Extrai apenas números e ponto
            match = re.search(r"[\d\.]+", clean)

            if match:
                num = float(match.group())
                return num if num > 0 else None
            return None

        except Exception:
            return None
