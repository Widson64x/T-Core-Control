import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from cachetools import cached, TTLCache

# Carrega variáveis do .env
load_dotenv()

# --- Configuração da Conexão ---
HOST = os.getenv("DB_HOST", "localhost")
PORT = os.getenv("DB_PORT", "5432")
DATABASE = os.getenv("DB_NAME", "DRE_Database")
USER = os.getenv("DB_USER", "postgres")
PASSWORD = os.getenv("DB_PASSWORD") 
DRIVER = os.getenv("DB_DRIVER", "psycopg")
DATABASE_URL = f"postgresql+{DRIVER}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

# --- Configuração do Cache ---
cache = TTLCache(maxsize=1, ttl=3600)

@cached(cache) 
def Carregar_Mapeamento_Banco():
    """
    Carrega TODAS as tabelas De-Para do PostgreSQL.
    """
    print("ATENÇÃO: Lendo dados do banco de dados (execução de cache)...")
    
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    except Exception as e:
        print(f"Erro ao criar a engine de conexão do SQLAlchemy: {e}")
        raise 
        
    mapeamentos = {}
    
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
        "De_Para_Grupos_Ocupacao": '"Tb_De_Para_Grupos_Ocupacao"',
        "Volumes_De_Para_Abreviacao3": '"Tb_Volumes_De_Para_Abreviacao3"'
    }

    nome_tabela_sql = "N/A"
    try:
        with engine.connect() as conn:
            for chave_esperada, nome_tabela_sql in mapa_tabelas.items():
                print(f"Carregando tabela: {nome_tabela_sql}...")
                mapeamentos[chave_esperada] = pd.read_sql(f"SELECT * FROM {nome_tabela_sql}", conn)
        
        print(f" -> {len(mapeamentos)} tabelas carregadas.")
        return mapeamentos
        
    except Exception as e:
        print(f"ERRO ao carregar mapeamentos: {e}")
        raise Exception(f"Erro ao carregar dados do banco: {e}") from e

def Atualizar_Sigla_Depositante(nome_depositante, nova_sigla):
    """
    NOVO: Atualiza a sigla (area) na tabela 2 se encontrar um depositante com cadastro incompleto.
    """
    try:
        engine = create_engine(DATABASE_URL)
        # SQL de Update seguro
        sql = text("""
            UPDATE "Tb_Volumes_De_Para_Abreviacao2"
            SET area = :sigla
            WHERE nome = :nome
        """)
        
        with engine.begin() as conn:
            conn.execute(sql, {"sigla": nova_sigla, "nome": nome_depositante})
            
        print(f"   [DB WRITE] Sucesso! Atualizado no Banco: '{nome_depositante}' -> '{nova_sigla}'")
        return True
    except Exception as e:
        print(f"   !!! [DB ERROR] Falha ao atualizar sigla no banco: {e}")
        return False

# Bloco de teste
if __name__ == "__main__":
    print("--- Teste Conexão ---")
    Carregar_Mapeamento_Banco()