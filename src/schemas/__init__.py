"""
Modelos de dados e validação do sistema.
Contém os schemas do Pydantic que representam a estrutura de saída esperada (Licitações e Itens).
"""

from src.schemas.licitacao import ItemLicitacao, Licitacao

__all__ = ["ItemLicitacao", "Licitacao"]
