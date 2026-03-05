import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TypedDict

from src.schemas import ItemLicitacao

logger = logging.getLogger(__name__)


class ExtractionState(TypedDict):
    last_lote: str | None
    item_counter: int
    current_header_map: dict[str, int] | None
    pending_broken_desc: str | None
    last_extracted_item: ItemLicitacao | None
    pending_item_num: int | None


class BaseExtractor(ABC):
    """
    Classe base para os extratores específicos.
    """

    @abstractmethod
    def extract(self, file_path: Path) -> list[ItemLicitacao]:
        pass
