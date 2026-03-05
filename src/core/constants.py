# =========================================================================
# MAPEAMENTOS E ESTRUTURAS PRINCIPAIS
# =========================================================================

# Mapeamento de possíveis nomes de colunas para os campos necessários
HEADER_MAPPING = {
    "item": ["item", "it"],
    "quantidade": ["quantidade", "quant", "qtd", "qtdd", "qtde", "qte", "unidades"],
    "objeto": [
        "objeto",
        "descricao",
        "descrição",
        "especificacao",
        "especificação",
        "especificacoes",
        "especificações",
        "discriminacao",
        "discriminação",
        "servico",
        "natureza",
        "produto",
    ],
    "unidade_fornecimento": ["unidade", "unid", "und", "undd", "u.m.", "un", "un."],
    "lote": ["lote", "grupo"],
}


# =========================================================================
# LIMITES DIMENSIONAIS E NUMÉRICOS (THRESHOLDS)
# =========================================================================

# Comprimento máximo aceitável para uma descrição
MAX_DESC_LEN = 1500

# Tamanho mínimo absoluto para uma string ser considerada uma descrição de produto válida
MIN_DESC_LEN = 3

# Tamanho mínimo da descrição se ela parecer ser apenas um número
MIN_DESC_NUMBER_LEN = 15

# Tamanho máximo aceitável para o texto de uma Unidade de Fornecimento
MAX_UNID_LEN = 20

# Limite máximo de quantidade para evitar a extração acidental de códigos
MAX_VALID_QUANTITY = 100000


# =========================================================================
# REGRAS DE DOMÍNIO (UNIDADES E LOTES)
# =========================================================================

# Unidades preteridas na deduplicação se houver uma alternativa mais específica no mesmo item
GENERIC_UNITS = frozenset({"UNIDADE"})

# Unidades de fornecimento explícitas usadas no resgate visual caso a coluna da unidade suma
VALID_UNIDS_SET = frozenset(
    [
        "UND",
        "UNID",
        "UNIDADE",
        "CX",
        "CAIXA",
        "KG",
        "L",
        "LITRO",
        "LITROS",
        "PCT",
        "PACOTE",
        "PAR",
        "PARES",
        "M",
        "METRO",
        "M2",
        "M3",
        "MÊS",
        "MESES",
        "SERVIÇO",
        "SERVICO",
        "CJ",
        "CONJUNTO",
        "KIT",
        "GALÃO",
        "FRASCO",
        "HORA SERVIÇO TECNICO",
        "ROLO",
        "TONELADA",
        "TON",
        "JG",
        "JOGO",
        "PEÇA",
        "PÇ",
        "UND SERVIÇO TÉCNICO",
    ]
)

# Palavras-chave que indicam endereços, bloqueando a falsa detecção de um "Lote"
LOTE_BLOCK_WORDS = frozenset(
    [
        "RUA",
        "QUADRA",
        "QDA",
        "BAIRRO",
        "CEP",
        "ENDEREÇO",
        "ENDERECO",
        "AVENIDA",
        "ALAMEDA",
        "RODOVIA",
    ]
)


# =========================================================================
# FILTROS DE RUÍDO E LIXO (GARBAGE / TRASH)
# =========================================================================

# Caracteres isolados que indicam descrições ou unidades corrompidas em tabelas mal formatadas
GARBAGE_CHARS = frozenset({"-", "_", ".", " ", "x"})

# Prefixos jurídicos/administrativos indicando que a linha é cláusula do edital, não um produto
INVALID_DESC_PREFIXES = ("art.", "lei ", "decreto", "data:", "assinatura", "pregão", "processo")

# Palavras que acionam o "Kill Switch" se encontradas dentro da coluna de descrição
GARBAGE_DESC_WORDS = frozenset(
    [
        "ANEXO",
        "TERMO DE",
        "DECLARAÇÃO",
        "EDITAL",
        "PREGÃO",
        "RODAPÉ",
        "CONSEQUÊNCIA",
        "RISCO",
        "SANÇÕES",
    ]
)

# Palavras e trechos que indicam que a linha é um rodapé, cabeçalho ou metadado do arquivo
TRASH_KEYWORDS = frozenset(
    [
        "cnpj:",
        "cep:",
        "telefone:",
        "tel:",
        "endereço:",
        "e-mail:",
        "email:",
        "valor total estimado",
        "documento assinado",
        "código verificador",
        "autenticidade do",
        "eproc.",
        "local e data",
        "valor máximo",
    ]
)

# Termos sem espaço que indicam linhas de totalizadores financeiros que devem ser ignoradas
FINANCIAL_TOTALS_KEYS = frozenset(
    [
        "valortotal",
        "valorglobal",
        "valormáximo",
        "xxxx",
    ]
)
