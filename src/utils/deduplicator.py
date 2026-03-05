from src.core import GARBAGE_CHARS, GENERIC_UNITS, MIN_DESC_LEN
from src.schemas import ItemLicitacao
from src.utils import normalize_lote


def deduplicate_items(items: list[ItemLicitacao]) -> list[ItemLicitacao]:
    """
    Agrupa itens extraídos pelo número do Lote e Item, mesclando dados de linhas
    espelhadas/repetidas para formar o item mais completo possível.

    Args:
        items (list[ItemLicitacao]): Lista bruta de itens extraídos.

    Returns:
        list[ItemLicitacao]: Lista de itens deduplicados, fundidos e ordenados.
    """
    if not items:
        return []

    final_map = {}

    for item in items:
        if is_garbage_item(item):
            continue

        lote = normalize_lote(item.lote)

        id_key = (lote, item.item)

        if id_key not in final_map:
            final_map[id_key] = item
        else:
            merge_duplicate_items(final_map[id_key], item)

    # Ordena a lista final priorizando o Lote, depois o ID do item
    return sorted(
        final_map.values(),
        key=lambda x: (
            int(x.lote) if x.lote and str(x.lote).isdigit() else 999,
            x.item if x.item is not None else 99999,
        ),
    )


def is_garbage_item(item: ItemLicitacao) -> bool:
    """
    Verifica se o item é um falso positivo baseado no tamanho e conteúdo da descrição.

    Args:
        item (ItemLicitacao): O item a ser avaliado.

    Returns:
        bool: True se o item for considerado lixo, False caso contrário.
    """
    if not item.objeto or len(item.objeto) < MIN_DESC_LEN:
        return True

    # Se a descrição for composta APENAS por caracteres inúteis, é lixo
    return set(item.objeto.strip().lower()) <= GARBAGE_CHARS


def merge_duplicate_items(existing: ItemLicitacao, new_item: ItemLicitacao) -> None:
    """
    Mescla dois itens de mesmo ID, priorizando a informação mais rica.
    Aplica regras de negócio para salvar a maior descrição e a melhor unidade.

    Args:
        existing (ItemLicitacao): O item já existente no mapa de resultados.
        new_item (ItemLicitacao): O novo item a ser comparado e possivelmente mesclado.
    """
    # Prioriza a descrição mais longa, assumindo que é mais detalhada e informativa
    if len(new_item.objeto) > len(existing.objeto):
        existing.objeto = new_item.objeto

    # Prioriza quantidades reais, protegendo contra None ou 0 da primeira leitura
    if (not existing.quantidade or existing.quantidade <= 1) and (
        new_item.quantidade and new_item.quantidade > 1
    ):
        existing.quantidade = new_item.quantidade

    # Refina a Unidade de Fornecimento
    new_u = str(new_item.unidade_fornecimento).strip().upper()
    old_u = str(existing.unidade_fornecimento).strip().upper()

    # Prioriza Unidades de Fornecimento mais específicas e detalhadas
    if (old_u in GENERIC_UNITS and new_u not in GENERIC_UNITS) or (
        old_u not in GENERIC_UNITS
        and new_u not in GENERIC_UNITS
        and len(new_item.unidade_fornecimento) > len(existing.unidade_fornecimento)
    ):
        existing.unidade_fornecimento = new_item.unidade_fornecimento

    if new_item.lote and not existing.lote:
        existing.lote = new_item.lote
