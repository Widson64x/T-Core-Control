from Db.Connection import Carregar_Mapeamento_Banco

class ServicoDePara:
    """
    Substitui a antiga classe 'De_Para'.
    Classe de serviço para encapsular o acesso aos mapeamentos.
    """
    
    @staticmethod
    def get_mapeamentos():
        """
        Retorna o dicionário de mapeamentos cacheados do banco.
        
        Substitui:
        - De_Para.iniciar_de_para()
        - De_Para.get_caminhos()
        - De_Para.arquivos_de_para()
        """
        try:
            # Chama a função cacheada de Connection.py
            mapeamentos = Carregar_Mapeamento_Banco()
            if mapeamentos is None:
                raise Exception("Carregar_Mapeamento_Banco retornou None.")
            
            # Garante que a coluna 'Grupo' em Contas Contabeis esteja em maiúsculo
            # (Replicando a lógica da linha 925 do seu DRE.PY)
            if "DRE_De_Para_Contas_Contabeis" in mapeamentos:
                df = mapeamentos["DRE_De_Para_Contas_Contabeis"]
                if "grupo" in df.columns:
                    df["grupo"] = df["grupo"].astype(str).str.strip().str.upper()
            
            return mapeamentos
        
        except Exception as e:
            print(f"Erro fatal no ServicoDePara: {e}")
            raise # Levanta a exceção para a rota tratar