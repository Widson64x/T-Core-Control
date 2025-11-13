from Db.Connection import Carregar_Mapeamento_Banco

class ServicoDePara:
    """
    Responsável apenas por buscar as tabelas de mapeamento (De-Para).
    """
    
    @staticmethod
    def get_mapeamentos():
        """
        Retorna o dicionário de DataFrames carregados do PostgreSQL.

        Substitui:
        - De_Para.iniciar_de_para()
        - De_Para.get_caminhos()
        - De_Para.arquivos_de_para()
  
        """
        try:
            # Chama a função do Connection.py que possui cache (TTLCache)
            # Isso evita ir ao banco de dados em toda requisição se não passou 1 hora.
            mapeamentos = Carregar_Mapeamento_Banco()
            
            if mapeamentos is None:
                raise Exception("Carregar_Mapeamento_Banco retornou None.")
            
            # REGRA DE NEGÓCIO ESPECÍFICA:
            # A tabela de Contas Contábeis precisa que a coluna 'grupo' esteja sempre
            # em maiúsculo e sem espaços nas pontas para o 'merge' funcionar depois.
            if "DRE_De_Para_Contas_Contabeis" in mapeamentos:
                df = mapeamentos["DRE_De_Para_Contas_Contabeis"]
                if "grupo" in df.columns:
                    df["grupo"] = df["grupo"].astype(str).str.strip().str.upper()
            
            return mapeamentos
        
        except Exception as e:
            print(f"Erro fatal no ServicoDePara: {e}")
            raise