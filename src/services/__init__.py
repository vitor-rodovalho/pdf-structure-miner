"""
Serviços de orquestração e pipelines do sistema.
Responsável por conectar a entrada de arquivos, aplicar as regras de negócio e gerenciar o fluxo
até a persistência dos dados.
"""

from src.services.orchestrator import Orchestrator
from src.services.pipeline import ExtractionPipeline

__all__ = ["ExtractionPipeline", "Orchestrator"]
