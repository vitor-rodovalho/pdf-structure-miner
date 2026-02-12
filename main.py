import argparse
import logging
import sys
from pathlib import Path

from src.core.logger_config import setup_logging
from src.services.orchestrator import Orchestrator

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
    Define e faz o parsing dos argumentos da linha de comando.
    """
    parser = argparse.ArgumentParser(
        description="Pipeline ETL para extração de dados de licitações públicas.",
        epilog="Exemplo: python main.py --input data/downloads --output data/output/resultado.json",
    )

    # [cite_start]Requisito 4.1: Entrada deve ser o caminho da pasta downloads [cite: 72]
    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        required=True,
        help="Caminho para a pasta contendo os arquivos JSON e anexos (ex: data/downloads).",
    )

    # [cite_start]Requisito 4.2: Saída deve ser um arquivo JSON [cite: 74]
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("data/output/resultado.json"),
        help="Caminho completo onde o arquivo JSON final será salvo.",
    )

    # Flag opcional para debug
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Ativa logs detalhados (DEBUG) no console.",
    )

    return parser.parse_args()


def main():

    # Parsing dos Argumentos
    args = parse_arguments()

    # Configuração de Logs
    setup_logging(verbose=args.verbose)

    logger.info("Iniciando Pipeline ETL de Licitações...")
    logger.info(f"Diretório de Entrada: {args.input}")
    logger.info(f"Arquivo de Saída: {args.output}")

    # 3. Validação de Entrada (Fail Fast)
    if not args.input.exists() or not args.input.is_dir():
        logger.critical(
            f"O diretório de entrada '{args.input}' não existe ou não é uma pasta."
        )
        sys.exit(1)

    # 4. Execução do Orquestrador (O Coração do Processo)
    try:
        # Instancia o orquestrador passando os caminhos configurados
        orchestrator = Orchestrator(input_path=args.input, output_path=args.output)

        # Dispara o processamento
        # O método run() deve encapsular o fluxo: Ler Pastas -> Extrair -> Transformar -> Salvar
        orchestrator.run()

        logger.info("Processamento concluído com sucesso!")
        sys.exit(0)

    except Exception as e:
        # Tratamento de Erro Top-Level
        # Captura qualquer erro não tratado para evitar crash feio no terminal
        logger.critical(f"Erro fatal durante a execução do pipeline: {e}")

        if args.verbose:
            logger.exception(
                "Detalhes do erro:"
            )  # Imprime stack trace se estiver em modo verbose

        sys.exit(1)


if __name__ == "__main__":
    main()
