import os

# Define o diretório base para os arquivos de entrada
# Altere este caminho para o local onde seus arquivos Excel estão
BASE_PATH = r"C:\\Projetos\\DRE\\Relatórios\\DREs"

# Mapeamento dos caminhos dos arquivos e seus parâmetros
CAMINHOS_ARQUIVOS = {
    "Volumes_Base": {
        "path": os.path.join(BASE_PATH, "Volumes - Base.xlsx"),
        "columns": ["SITE", "CLIENTE", "DATAFIMPEDIDO", "CATEGORIAGRUPO", "VOLUMES"]
    },
    "Faturamento": {
        "path": os.path.join(BASE_PATH, "Faturamento 2025.xlsx"),
        "columns": ["EMPRESA", "FILIAL", "CLIENTE", "RECEITA", "VERSÃO", "MÊS", "ANO", "TIPO", "VALOR R$"],
        "sheet_name": "base",
        "header": 6, # Ajustado de [6] para 6, como pandas espera
        "empresa": ["FARMA", "FARMA DIST"],
        "ano": [2025],
        "versao": ["Real"],
        "receita": ["Serviços"]
    },
    "Insumos": {
        "path": os.path.join(BASE_PATH, "Insumos.xlsx"),
        "columns": ["ID", "Mês", "Depositante", "NOMECLI", "Custo", "Insumo"]
    },
    "Adequacao": {
        "path": os.path.join(BASE_PATH, "Quantidade - Adequação.xlsx"),
        "columns": ["Filial", "Cliente", "Qtde Real", "Nome Servico", "Serviço", "Data Fim"]
    },
    "DRE": {
        "path": os.path.join(BASE_PATH, "Resultado DRE Mensal 2025_v2.xlsx"),
        "sheet_name": ["RAZÃO_FARMA", "RAZÃO_FARMADIST"],
        "header": 3, # Ajustado de [3] para 3
        "colunas_dre": ["Conta", "Título Conta", "Data", "Descrição", "Filial", "Centro de Custo", "Item", "saldo"],
        "colunas_str": ["Conta", "Item", "Filial", "Centro de Custo"]
    },
    "Ocupacao_Armazem": {
        "path": os.path.join(BASE_PATH, "Acompanhamento Pallets 2025.xlsx"),
        "sheet_name": ["SP", "SC", "RJ", "GO"],
        "header": [4, 5], # Header multi-index
        "columns": [
            "Mês",
            "Cliente",
            "Ocupação",
            "Filial",
            "Ano"
        ],
        "escrita_mes": "Mês"
    }
}