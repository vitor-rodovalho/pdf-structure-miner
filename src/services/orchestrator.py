import json
import logging
from pathlib import Path

from unidecode import unidecode

from src.extractors.base import BaseExtractor
from src.extractors.docx import DocxExtractor
from src.extractors.pdf import PDFExtractor
from src.schemas.licitacao import Licitacao

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Classe responsável por orquestrar o processo de leitura dos arquivos JSON, encontrar as pastas
    de anexos, e delegar a extração dos dados para os extratores adequados, construindo o objeto
    Licitacao final.
    """

    FILE_PRIORITY_BREAK = 100

    def __init__(self):
        """
        Inicializa os extratores suportados.
        """
        # Inicializa os extratores uma única vez
        self.extractors = {".pdf": PDFExtractor(), ".docx": DocxExtractor()}


    def process_directory(self, input_dir: Path) -> list[Licitacao]:
        """
        Varre o diretório buscando pares de JSON + Pasta de Anexos.

        Args:
            input_dir (Path): Diretório raiz onde estão os arquivos JSON e as pastas de anexos.

        Returns:
            list[Licitacao]: Lista de objetos Licitacao processados.
        """

        results = []
        # Encontra todos os JSONs na pasta raiz
        json_files = list(input_dir.glob("*.json"))

        logger.info(f"Encontrados {len(json_files)} arquivos de licitação para processar.")

        for json_file in json_files:
            try:
                licitacao = self._process_single_licitacao(json_file)
                results.append(licitacao)
            except Exception as e:
                logger.error(f"Falha crítica ao processar {json_file.name}: {e}")

        return results


    def _process_single_licitacao(self, json_path: Path) -> Licitacao:
        """
        Lê o JSON, encontra a pasta de anexos e processa os arquivos compatíveis.

        Args:
            json_path (Path): Caminho para o arquivo JSON da licitação.

        Returns:
            Licitacao: Objeto contendo os dados extraídos da licitação.
        """
        logger.info(f"=== Processando JSON: {json_path.name} ===")

        # Lê Metadados do JSON
        with json_path.open(mode="r", encoding="utf-8") as f:
            data = json.load(f)

        # Extrai campos básicos para o esquema final
        meta = data.get("data", {})

        # Encontra pasta de anexos
        attachments_dir = json_path.with_suffix("")

        processed_files = []
        all_items = []

        if attachments_dir.exists() and attachments_dir.is_dir():

            files_with_priority = []
            # Filtra apenas os arquivos que têm extratores e calcula a prioridade
            for f in attachments_dir.iterdir():
                if f.is_file() and f.suffix.lower() in self.extractors:
                    score = self._calculate_file_priority(f)
                    files_with_priority.append((score, f))

            # Ordena pelo primeiro elemento da tupla (o Score)
            files_with_priority.sort(key=lambda x: x[0], reverse=True)

            logger.debug(f"Ordem: {[f.name for _, f in files_with_priority]}")

            for priority, file_path in files_with_priority:

                extractor = self._get_extractor(file_path)

                if extractor:
                    try:
                        items = extractor.extract(file_path)
                        if items:
                            all_items.extend(items)
                            processed_files.append(file_path.name)

                            if priority >= self.FILE_PRIORITY_BREAK:
                                logger.info(f"Documento de alta prioridade encontrado " \
                                            f"({file_path.name}). Finalizando busca.")
                                break

                    except Exception as e:
                        logger.warning(f"Falha ao processar arquivo {file_path}: {e}")
        else:
            logger.warning(f"Pasta de anexos não encontrada: {attachments_dir}")

        final_items = self._deduplicate_items(all_items)

        # Constrói e retorna o objeto Licitacao final
        return Licitacao(
            arquivo_json=json_path.name,
            numero_pregao=str(meta.get("numero_pregao", "")),
            orgao=str(meta.get("orgao", "")),
            cidade=str(meta.get("cidade", "")),
            estado=str(meta.get("estado", "")),
            anexos_processados=processed_files,
            itens_extraidos=final_items,
        )


    def _get_extractor(self, file_path: Path) -> BaseExtractor | None:
        """
        Retorna o extrator adequado baseado na extensão, ou None caso a extensão não seja suportada.

        Args:
            file_path (Path): Caminho do arquivo a ser processado.

        Returns:
            BaseExtractor | None: Instância do extrator ou None se não suportado.
        """
        return self.extractors.get(file_path.suffix.lower())


    def _calculate_file_priority(self, file_path: Path) -> int:
        """
        Calcula um score de prioridade baseado no nome do arquivo.
        Quanto maior o número, antes o arquivo deve ser processado.

        Args:
            file_path (Path): Caminho do arquivo a ser avaliado.

        Returns:
            int: Score de prioridade (0-100).
        """
        # Normaliza o nome: minúsculo e sem acentos
        name = unidecode(file_path.name.lower())

        # Prioridade Máxima: Relação de Itens
        if "-relacaoitens" in name:
            return 100

        # Prioridade Média: Termos de Referência
        if "termo" in name:
            return 75

        # Prioridade Média: Editais
        if "edital" in name:
            return 50

        # Prioridade Baixa: Outros anexos
        return 0


    def _deduplicate_items(self, items: list) -> list:
        """
        Remove itens duplicados mantendo a melhor versão.

        Args:
            items (list): Lista de itens extraídos, possivelmente com duplicatas.

        Returns:
            list: Lista de itens únicos, ordenados por número do item.
        """

        unique_map = {}
        for item in items:
            # Chave composta: Número do item + Início da descrição
            key = (item.item, item.objeto[:30].lower())

            if key not in unique_map:
                unique_map[key] = item
            else:
                # Se já existe, substitui APENAS se o novo tiver Lote e o antigo não
                existing = unique_map[key]
                if item.lote and not existing.lote:
                    unique_map[key] = item

        # Retorna ordenado pelo número do item
        return sorted(unique_map.values(), key=lambda x: x.item)
