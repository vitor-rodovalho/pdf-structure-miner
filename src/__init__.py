"""
Sistema de Extração e Processamento de Licitações (ETL).

Este projeto é responsável por orquestrar a leitura de editais, termos de
referência e anexos (PDFs, DOCXs e ZIPs), aplicando heurísticas de extração
para estruturar os dados de itens e lotes em um formato JSON padronizado.

Módulos principais:
    - core: Configurações e regras universais de negócio.
    - extractors: Leitores e interpretadores de formatos específicos (PDF, DOCX).
    - parsers: Motores de interpretação de tabelas e textos.
    - schemas: Definição de tipos e validação de dados (Pydantic).
    - services: Orquestração e pipeline de execução.
    - utils: Ferramentas puras de higienização de dados.
"""
