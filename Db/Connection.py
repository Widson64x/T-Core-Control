import os
import pandas as pd
from sqlalchemy import create_engine, types
from dotenv import load_dotenv
from cachetools import cached, TTLCache

# Carrega variáveis do .env
load_dotenv()

# --- Configuração da Conexão ---
HOST = os.getenv("DB_HOST", "localhost")
PORT = os.getenv("DB_PORT", "5432")
DATABASE = os.getenv("DB_NAME", "DRE_Database")
USER = os.getenv("DB_USER", "postgres")
PASSWORD = os.getenv("DB_PASSWORD") # Mantenha a senha codificada
DRIVER = os.getenv("DB_DRIVER", "psycopg")
DATABASE_URL = f"postgresql+{DRIVER}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

# --- Configuração do Cache ---
# Cache na memória com 1 item, expira em 1 hora (3600 segundos)
cache = TTLCache(maxsize=1, ttl=3600)

@cached(cache) # Aplica o cache
def Carregar_Mapeamento_Banco():
    """
    Carrega TODAS as tabelas De-Para do PostgreSQL e as armazena
    em um dicionário cacheado.
    """
    print("ATENÇÃO: Lendo dados do banco de dados (execução de cache)...")
    
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    except Exception as e:
        print(f"Erro ao criar a engine de conexão do SQLAlchemy: {e}")
        raise 
        
    mapeamentos = {}
    
    # Adicionadas as aspas duplas para nomes de tabela case-sensitive
    mapa_tabelas = {
        "DRE_De_Para_Item_Conta": '"Tb_DRE_De_Para_Item_Conta"',
        "DRE_De_Para_Centro_Custo": '"Tb_DRE_De_Para_Centro_Custo"',
        "DRE_De_Para_Filial": '"Tb_DRE_De_Para_Filial"',
        "DRE_De_Para_Contas_Contabeis": '"Tb_DRE_De_Para_Contas_Contabeis"',
        "Volumes_De_Para_Abreviacao": '"Tb_Volumes_De_Para_Abreviacao"',
        "Embalagens_De_Para_Clientes": '"Tb_Embalagens_De_Para_Clientes"',
        "MO_Ade_Temp_Filial_UF": '"Tb_MO_Ade_Temp_Filial_UF"',
        "MO_Ade_Temp_Cli_Grupo": '"Tb_MO_Ade_Temp_Cli_Grupo"',
        "Item_De_Para_Filial_Depreciacao": '"Tb_Item_De_Para_Filial_Depreciacao"',
        "De_Para_Grupos_Ocupacao": '"Tb_De_Para_Grupos_Ocupacao"'
    }

    nome_tabela_sql = "N/A (Erro na conexão inicial)"
    try:
        with engine.connect() as conn:
            for chave_esperada, nome_tabela_sql in mapa_tabelas.items():
                print(f"Carregando tabela: {nome_tabela_sql}...")
                mapeamentos[chave_esperada] = pd.read_sql(f"SELECT * FROM {nome_tabela_sql}", conn)
        
        print(f" -> {len(mapeamentos)} tabelas de mapeamento carregadas com sucesso do banco.")
        return mapeamentos
        
    except Exception as e:
        print(f"ERRO ao carregar mapeamentos da tabela {nome_tabela_sql}: {e}")
        raise Exception(f"Erro ao carregar dados do banco: {e}. A tabela '{nome_tabela_sql}' existe?") from e
    
# Bloco de teste
if __name__ == "__main__":
    print("--- INICIANDO TESTE DE CONEXÃO (Connection.py) ---")
    mapeamentos = Carregar_Mapeamento_Banco()
    if mapeamentos:
        print("\n--- SUCESSO! ---")
        print(f"Banco de dados conectado e {len(mapeamentos)} tabelas carregadas!")
    else:
        print("\n--- FALHA! ---")
    print("--- TESTE CONCLUÍDO ---")