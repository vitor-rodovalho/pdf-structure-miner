import argparse
import logging
import sys
from pathlib import Path

from src.core.logger_config import setup_logging
from src.services.pipeline import ExtractionPipeline

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """
    Define e faz o parsing dos argumentos da linha de comando.

    Returns:
        argparse.Namespace: Objeto contendo os argumentos parseados.
    """
    parser = argparse.ArgumentParser(
        description="Pipeline para extração de dados de licitações públicas.",
        epilog="Exemplo: python main.py --input data/downloads --output data/output/resultado.json",
    )

    parser.add_argument(
        "--input",
        "-i",
        type=Path,
        required=True,
        help="Caminho para a pasta contendo os arquivos JSON e pastas com anexos "
        "(ex: data/downloads).",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        required=True,
        help="Caminho completo onde o arquivo array JSON final será salvo.",
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
    """
    Função principal que inicia o pipeline de extração de dados de licitações.

    Fluxo de execução:
    1. Parsing dos argumentos da linha de comando.
    2. Configuração do sistema de logs.
    3. Execução do pipeline de extração.
    4. Tratamento de erros e saída apropriada.

    Raises:
        FileNotFoundError: Se o diretório de entrada não existir.
        NotADirectoryError: Se o caminho de entrada não for uma pasta.
        Exception: Qualquer outro erro crítico durante a execução do pipeline.
    """

    # Parsing dos Argumentos
    args = parse_arguments()

    # Configuração de Logs
    setup_logging(verbose=args.verbose)

    try:
        # Execução do Pipeline
        pipeline = ExtractionPipeline()
        pipeline.run(input_path=args.input, output_path=args.output)

        sys.exit(0)

    except FileNotFoundError as e:
        logger.critical(str(e))
        sys.exit(1)

    except NotADirectoryError as e:
        logger.critical(str(e))
        sys.exit(1)

    except Exception as e:
        logger.critical(f"Erro fatal durante a execução do pipeline: {e}")

        # Imprime stack trace se estiver em modo verbose
        if args.verbose:
            logger.exception("Detalhes do erro:")

        sys.exit(1)


if __name__ == "__main__":
    main()
