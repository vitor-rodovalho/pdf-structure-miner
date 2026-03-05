import re
from collections.abc import Sequence

from src.core import MAX_UNID_LEN

# Máximo de letras permitidas dentro de uma string para ser tentada como número no fallback
MAX_ALPHA_IN_NUMBER = 2


def clean_number(val: str | None) -> float | None:
    """
    Extrai o primeiro número válido de uma string ruidosa.
    Aborta se a string contiver excesso de letras, indicando ser um texto comum.

    Args:
        val (str | None): String ruidosa extraída de uma célula.

    Returns:
        float | None: Número decimal/inteiro ou None se a string for inválida.
    """
    if not val:
        return None

    try:
        clean = val.strip()

        # Impede a extração de números dentro de textos puramente descritivos
        if len(re.findall(r"[a-zA-Z]", clean)) > MAX_ALPHA_IN_NUMBER:
            return None

        clean = clean.replace(".", "").replace(",", ".")
        match = re.search(r"[\d\.]+", clean)

        if match:
            num = float(match.group())
            return num if num > 0 else None

        return None

    except Exception:
        return None


def clean_unidade_fornecimento(val: str | None) -> str:
    """
    Higieniza a unidade de fornecimento.
    Esta função remove números e pontuações iniciais para isolar apenas o texto da unidade.

    Args:
        val (str | None): Valor bruto extraído da coluna de unidade.

    Returns:
        str: Unidade de fornecimento limpa (ou "Unidade" como padrão caso inválida).
    """
    if not val:
        return "Unidade"

    # Remove dígitos, pontos, vírgulas e espaços do início da string
    cleaned = re.sub(r"^[\d\.,\s]+", "", str(val)).strip()

    # Se após a limpeza não sobrar nada, ou sobrar um texto gigante, assumime o valor padrão
    if not cleaned or len(cleaned) > MAX_UNID_LEN:
        return "Unidade"

    return cleaned


def normalize_lote(val: str | int | None) -> str | None:
    """
    Remove zeros à esquerda de lotes puramente numéricos.

    Args:
        val (str | int | None): Valor bruto do lote.

    Returns:
        str | None: Lote formatado.
    """
    if not val:
        return None

    s = str(val).strip()

    if s.isdigit():
        return str(int(s))

    return s


def clean_rows(rows: Sequence[Sequence[str | None]]) -> list[list[str]]:
    """
    Limpa uma matriz bruta de linhas, removendo quebras de linha indesejadas e descartando
    linhas que estejam completamente vazias.

    Args:
        rows (Sequence[Sequence[str | None]]): Matriz bidimensional extraída da tabela.

    Returns:
        list[list[str]]: Matriz bidimensional higienizada, contendo apenas strings.
    """
    cleaned = []

    for row in rows:
        # Substitui quebras de linha por espaço e remove espaços extras
        clean_row = [(cell.replace("\n", " ").strip() if cell else "") for cell in row]

        # Só adiciona a linha se houver pelo menos uma célula preenchida
        if any(clean_row):
            cleaned.append(clean_row)

    return cleaned


def get_text_safe(row: Sequence[str | None], idx: int | None) -> str:
    """
    Extrai o texto de uma célula da linha de forma segura, tratando índices inválidos
    e quebras de linha.

    Args:
        row (Sequence[str | None]): Linha de dados atual.
        idx (int | None): Índice da coluna a ser extraída.

    Returns:
        str: Texto limpo da célula ou string vazia se inválido.
    """
    if idx is not None and 0 <= idx < len(row):
        val = row[idx]

        if val is not None:
            return str(val).replace("\n", " ").strip()

    return ""
