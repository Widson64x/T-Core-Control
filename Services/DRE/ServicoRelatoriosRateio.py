import pandas as pd
import re

class ServicoRelatoriosRateio:
    """
    Refatoração da classe Relatorios_Rateio (linhas 137-1234 do DRE.PY).
    """
    
    def __init__(self, mapeamentos, caminhos):
        """
        Inicializa o serviço com os mapeamentos (do banco) e caminhos (do config).
        """
        self.mapeamentos = mapeamentos
        self.caminhos = caminhos
        
        self.nas_de_para_rateio = [] 
        self.alertas_tamanho = []

        if not self.mapeamentos:
            raise ValueError("Mapeamentos (g.mapeamentos) não foram carregados.")
        if not self.caminhos:
            raise ValueError("Caminhos de arquivos (config.py) não foram carregados.")

    def _validar_e_renomear_colunas(self, df, colunas_esperadas, nome_arquivo):
        """
        Substitui a lógica de 'st.selectbox' para colunas faltantes.
        Valida se as colunas existem (ignorando case/espaços)
        e AS RENOMEIA para o padrão definido em config.py.
        """
        # Mapeia NOME_MAIUSCULO_STRIPPED -> NOME_ORIGINAL (ex: 'data fim')
        df_cols_map = {str(col).upper().strip(): str(col) for col in df.columns}
        # Mapeia NOME_MAIUSCULO_STRIPPED -> NOME_ESPERADO (ex: 'Data Fim')
        colunas_esperadas_map = {str(col).upper().strip(): str(col) for col in colunas_esperadas}
        
        rename_map = {}
        colunas_padrao_para_retornar = []
        
        for col_upper, col_esperada in colunas_esperadas_map.items():
            if col_upper not in df_cols_map:
                # Se uma coluna não existe, o processo deve falhar
                raise ValueError(f"Coluna obrigatória '{col_esperada}' (de config.py) não encontrada no arquivo '{nome_arquivo}'. Colunas presentes: {list(df.columns)}")
            
            # Mapeia o nome original (ex: 'data fim') para o nome padrão (ex: 'Data Fim')
            nome_original_no_df = df_cols_map[col_upper]
            if nome_original_no_df != col_esperada:
                rename_map[nome_original_no_df] = col_esperada
            
            colunas_padrao_para_retornar.append(col_esperada) # Adiciona o nome padrão
        
        # Renomeia o DataFrame para o padrão
        if rename_map:
            df = df.rename(columns=rename_map)
        
        # Retorna o DataFrame renomeado e a lista de colunas padrão
        return df, colunas_padrao_para_retornar

    def carregar_volume(self):
        print("Processando: Volumes Base")
        cfg = self.caminhos["Volumes_Base"]
        
        try:
            df = pd.read_excel(cfg["path"])
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de Volumes Base não encontrado em: {cfg['path']}")
        
        # Valida e renomeia colunas
        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Volumes_Base")
        df = df[cols] # Seleciona apenas as colunas padrão
        # Renomeia para um padrão MAIÚSCULO para os merges
        df.columns = [str(c).upper().strip() for c in cols]


        print("Volumes Base - Transformando Datas...")
        try:
            df["DATAFIMPEDIDO"] = pd.to_datetime(df["DATAFIMPEDIDO"], errors='coerce', dayfirst=True)
        except Exception:
            df["DATAFIMPEDIDO"] = pd.to_datetime(
                df["DATAFIMPEDIDO"].astype(str).str.strip().str.split(" ").str[0],
                format="mixed", dayfirst=False, errors="coerce"
            )

        df["Tabela"] = "Relatório de Saída"
        df["Mês"] = df["DATAFIMPEDIDO"].dt.month.astype(str)
        df["Ano"] = df["DATAFIMPEDIDO"].dt.year.astype(str)

        tamanho_antes = len(df[df["VOLUMES"] != 0])
        df = df[df["VOLUMES"] != 0].merge(
            self.mapeamentos["Volumes_De_Para_Abreviacao"],
            how="left", left_on="CLIENTE", right_on="area"
        ).drop(columns="area")
        tamanho_depois = len(df)
        
        if tamanho_antes != tamanho_depois:
            self.alertas_tamanho.append(f"Volumes_De_Para_Abreviacao (em Volumes): {tamanho_antes} linhas antes vs {tamanho_depois} depois.")

        df = df.rename(columns={
            "SITE": "Filial UF", "CATEGORIAGRUPO": "Item",
            "VOLUMES": "saldo", "CLIENTE": "Area", "grupo": "Grupo"
        })

        if df["Grupo"].isna().any():
            self.nas_de_para_rateio.append(df[df["Grupo"].isna()])
            print(f"AVISO: {df['Grupo'].isna().sum()} linhas em Volumes não encontraram 'Grupo' no De-Para.")

        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)[["saldo"]].sum()
        df["Filial UF"] = df["Filial UF"].str.replace("ITJ", "SC")
        print("Processando: Volumes Base - Finalizado")
        return df
    
    def adequacao(self):
        print("Processando: Adequação")
        cfg = self.caminhos["Adequacao"]

        try:
            df = pd.read_excel(cfg["path"])
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de Adequação não encontrado em: {cfg['path']}")
            
        # Valida e renomeia colunas
        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Adequacao")
        df = df[cols]
        # Agora o df TEM a coluna "Data Fim", com o case correto.

        try:
            df["Data Fim"] = pd.to_datetime(df["Data Fim"], format="%d/%m/%Y %H:%M:%S")
        except:
            df["Data Fim"] = pd.to_datetime(
                df["Data Fim"].astype(str).str.strip().str.split(" ").str[0],
                format='mixed', dayfirst=True, errors="coerce"
            )

        df["Mês"] = df["Data Fim"].dt.month.astype(str)
        df["Ano"] = df["Data Fim"].dt.year.astype(str)
        df["Tabela"] = "Relatório de Adequação"

        tamanho_antes = len(df)
        df = df.merge(
            self.mapeamentos["Volumes_De_Para_Abreviacao"], 
            how="left", left_on="Cliente", right_on="area"
        ).drop(columns="area")
        tamanho_depois = len(df)
        
        if tamanho_antes != tamanho_depois:
            self.alertas_tamanho.append(f"Volumes_De_Para_Abreviacao (em Adequação): {tamanho_antes} linhas antes vs {tamanho_depois} depois.")

        if df["grupo"].isna().any():
            self.nas_de_para_rateio.append(df[df["grupo"].isna()])
            print(f"AVISO: {df['grupo'].isna().sum()} linhas em Adequação não encontraram 'Grupo' no De-Para.")

        df = df.rename(columns={
            "Serviço": "Area", "Nome Servico": "Item", 
            "Qtde Real": "saldo", "Filial": "Filial UF", "grupo": "Grupo"
        })
        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)[["saldo"]].sum()
        print("Processando: Adequação - Finalizado")
        return df

    def insumos(self):
        print("Processando: Insumos")
        cfg = self.caminhos["Insumos"]
        
        try:
            df = pd.read_excel(cfg["path"])
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de Insumos não encontrado em: {cfg['path']}")

        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Insumos")
        df = df[cols].loc[df["Custo"] != 0]

        df["Tabela"] = "Insumos"
        df["ID"] = df["ID"].astype(str)
        df["Ano"] = df["ID"].str[:4]

        # Merge 1: Embalagens_De_Para_Clientes
        tamanho_antes = len(df)
        df = df.merge(
            self.mapeamentos["Embalagens_De_Para_Clientes"], 
            how='left', left_on='NOMECLI', right_on='nome_cliente'
        )
        if len(df) != tamanho_antes:
            self.alertas_tamanho.append(f"Embalagens_De_Para_Clientes (em Insumos): {tamanho_antes} linhas antes vs {len(df)} depois.")
        if df["filial_uf"].isna().any():
            self.nas_de_para_rateio.append(df[df["filial_uf"].isna()])
            print(f"AVISO: {df['filial_uf'].isna().sum()} linhas em Insumos não encontraram 'Filial UF' no De-Para de Clientes.")

        # Merge 2: Volumes_De_Para_Abreviacao
        tamanho_antes = len(df)
        df = df.merge(
            self.mapeamentos["Volumes_De_Para_Abreviacao"], 
            how='left', left_on='Depositante', right_on='area'
        ).drop(columns="area")
        if len(df) != tamanho_antes:
            self.alertas_tamanho.append(f"Volumes_De_Para_Abreviacao (em Insumos): {tamanho_antes} linhas antes vs {len(df)} depois.")
        if df["grupo"].isna().any():
            self.nas_de_para_rateio.append(df[df["grupo"].isna()])
            print(f"AVISO: {df['grupo'].isna().sum()} linhas em Insumos não encontraram 'Grupo' no De-Para de Volumes.")

        df = df.rename(columns={
            "NOMECLI": "Area", "Insumo": "Item", "Custo": "saldo", 
            "filial_uf": "Filial UF", "grupo": "Grupo"
        })
        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)[["saldo"]].sum()
        df["saldo"] = df["saldo"] * 0.9075
        print("Processando: Insumos - Finalizado")
        return df

    def faturamento(self):
        print("Processando: Faturamento")
        cfg = self.caminhos["Faturamento"]
        
        try:
            df = pd.read_excel(
                cfg["path"],
                sheet_name=cfg["sheet_name"],
                header=cfg["header"]
            )
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de Faturamento não encontrado em: {cfg['path']}")
        except ValueError as e:
            raise ValueError(f"Erro ao ler arquivo de Faturamento (provavelmente aba '{cfg['sheet_name']}' não encontrada): {e}")

        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Faturamento")
        
        df = df[df["EMPRESA"].isin(cfg["empresa"]) &
                df["ANO"].isin(cfg["ano"]) &
                df["VERSÃO"].isin(cfg["versao"]) &
                df["RECEITA"].isin(cfg["receita"]) &
                df["VALOR R$"].notna()]
        
        df = df.groupby(["ANO", "MÊS", "EMPRESA", "FILIAL", "CLIENTE", "TIPO"], as_index=False)[["VALOR R$"]].sum()
        df["VALOR R$"] = df["VALOR R$"] * 0.9075
        df["ANO"] = df["ANO"].astype(str).str.split(r'[. ]').str[0]

        tamanho_antes = len(df)
        df = df.merge(
            self.mapeamentos["DRE_De_Para_Filial"], 
            how='left', left_on="FILIAL", right_on="filial_nome"
        )
        if len(df) != tamanho_antes:
             self.alertas_tamanho.append(f"DRE_De_Para_Filial (em Faturamento): {tamanho_antes} linhas antes vs {len(df)} depois.")
        if df["filial_uf"].isna().any():
            self.nas_de_para_rateio.append(df[df["filial_uf"].isna()])
            print(f"AVISO: {df['filial_uf'].isna().sum()} linhas em Faturamento não encontraram 'Filial UF' no De-Para.")

        df["Tabela"] = "Faturamento"
        df["Item"] = "Faturamento"
        df = df.rename(columns={
            "ANO": "Ano", "MÊS": "Mês", "CLIENTE": "Grupo",
            "TIPO": "Area", "VALOR R$": "saldo", "filial_uf": "Filial UF"
        })

        df = df[["Tabela", "Ano", "Mês", "Filial UF", "Grupo", "Area", "Item", "saldo"]]
        print("Processando: Faturamento - Finalizado")
        return df

    def ocupacao_armazem(self):
        print("Processando: Ocupação Armazém")
        cfg = self.caminhos["Ocupacao_Armazem"]
        abas = cfg["sheet_name"]
        df_final_ocupacao = pd.DataFrame()
        
        try:
            excel_file = pd.ExcelFile(cfg["path"])
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de Ocupação não encontrado em: {cfg['path']}")

        for i in abas:
            print(f"  - Processando aba: {i}")
            if i not in excel_file.sheet_names:
                raise ValueError(f"Aba '{i}' não encontrada no arquivo Ocupação Armazém.")
                
            Ocupacao_Armazem = pd.read_excel(excel_file, sheet_name=i, header=cfg["header"])
            month = cfg["escrita_mes"]
            
            Posicao_Palet_SP_Alterado = Ocupacao_Armazem.copy().fillna(0)
            
            # --- Início da Lógica de Multi-Index (Migrada de DRE.PY) ---
            colunas_para_remover = []
            for col in Posicao_Palet_SP_Alterado.columns:
                try:
                    if pd.api.types.is_numeric_dtype(Posicao_Palet_SP_Alterado[col]):
                        if Posicao_Palet_SP_Alterado[col].sum() == 0:
                            colunas_para_remover.append(col)
                except (TypeError, ValueError):
                    pass
            Posicao_Palet_SP_Alterado = Posicao_Palet_SP_Alterado.drop(columns=colunas_para_remover)

            Clientes_2 = []
            for column in Posicao_Palet_SP_Alterado.columns:
                novo_nome = column[0].replace("  ", " ").strip() if isinstance(column, tuple) else column.replace("  ", " ").strip()
                Clientes_2.append(novo_nome)
            
            Clientes_2 = pd.DataFrame(Clientes_2, columns=["Clientes"]).drop_duplicates()
            colunas_mes = [col for col in Posicao_Palet_SP_Alterado.columns if month in str(col[0]) or month in str(col[1])]
            if colunas_mes:
                Clientes_2 = Clientes_2.loc[(Clientes_2["Clientes"] != colunas_mes[0][0])]

            Colunas = Posicao_Palet_SP_Alterado.columns
            Registros_Totais = pd.DataFrame()
            Posicao_Pallet = pd.DataFrame()

            for outro in Colunas:
                df_subset = Posicao_Palet_SP_Alterado[outro[0]]
                if isinstance(df_subset, pd.DataFrame):
                    if len(df_subset.columns.values) > 1:
                        Registros_Totais[outro] = Posicao_Palet_SP_Alterado[outro]
                    else:
                        Posicao_Pallet[outro] = Posicao_Palet_SP_Alterado[outro]
                else:
                    Posicao_Pallet[outro] = Posicao_Palet_SP_Alterado[outro]

            if not Posicao_Pallet.empty:
                renomear = []
                for column in Posicao_Pallet.columns.values:
                    if isinstance(column, tuple):
                        name1, name2 = column
                        renomear.append(name2 if name2 == "Mês" else name1)
                    else:
                        renomear.append(column) 
                Posicao_Pallet.columns = renomear

                Posicao_Pallet = (Posicao_Pallet.set_index(["Mês"])
                                        .stack()
                                        .reset_index(name='Ocupação')
                                        .rename(columns={'level_1':'Cliente'}))
                Posicao_Pallet["Filial"] = i

            if not Registros_Totais.empty:
                Registros_Totais.columns = Registros_Totais.columns.map(lambda x: str(x).upper())
                Clientes_Sem_Total = Registros_Totais.loc[:, ~Registros_Totais.columns.str.contains("TOTAL", case=False, na=False)]
                if not Clientes_Sem_Total.empty:
                    Clientes_Sem_Total = Clientes_Sem_Total.T.groupby(level=0).sum().T
                
                Registros_Totais = Registros_Totais.loc[:, Registros_Totais.columns.str.contains("TOTAL", case=False, na=False)]
                Registros_Totais.columns = [str(col).replace("(", "").replace(")", "").replace("'", "") for col in Registros_Totais.columns]
                Registros_Totais.columns = [tuple(col.split(',')) for col in Registros_Totais.columns]
                
                renomear = [str(col[0]).strip() for col in Registros_Totais.columns]
                Registros_Totais.columns = renomear

                if not Clientes_Sem_Total.empty:
                    for col_total in Registros_Totais.columns.values:
                        Clientes_Sem_Total = Clientes_Sem_Total.loc[:, ~Clientes_Sem_Total.columns.str.contains(col_total, case=False, na=False)]
                    
                    Clientes_Sem_Total.columns = [str(col).replace("(", "").replace(")", "").replace("'", "") for col in Clientes_Sem_Total.columns]
                    Clientes_Sem_Total.columns = [tuple(col.split(',')) for col in Clientes_Sem_Total.columns]
                    renomear = [str(col[0]).strip() for col in Clientes_Sem_Total.columns]
                    Clientes_Sem_Total.columns = renomear
                    
                    Clientes_Sem_Total = Clientes_Sem_Total.T.groupby(level=0).sum().T
                    Registros_Totais = pd.concat([Registros_Totais, Clientes_Sem_Total], axis=1)

                if not Posicao_Pallet.empty and "Mês" in Posicao_Pallet.columns:
                     Registros_Totais["Mês"] = list(Posicao_Pallet["Mês"].unique())
                else:
                     col_mes_fallback = [col for col in Posicao_Palet_SP_Alterado.columns if 'Mês' in str(col[0]) or 'Mês' in str(col[1])]
                     if col_mes_fallback:
                         Registros_Totais["Mês"] = Posicao_Palet_SP_Alterado[col_mes_fallback[0]].values
                     else:
                         raise ValueError("Não foi possível encontrar a coluna 'Mês' para o pivot.")

                Registros_Totais = (Registros_Totais.set_index(["Mês"])
                                        .stack()
                                        .reset_index(name='Ocupação')
                                        .rename(columns={'level_1':'Cliente'}))
                Registros_Totais["Filial"] = i

            if not Posicao_Pallet.empty and not Registros_Totais.empty:
                Posicao_Palet_SP_Alterado = pd.concat([Posicao_Pallet, Registros_Totais])
            elif Posicao_Pallet.empty and not Registros_Totais.empty:
                Posicao_Palet_SP_Alterado = Registros_Totais.copy()
            elif not Posicao_Pallet.empty and Registros_Totais.empty:
                Posicao_Palet_SP_Alterado = Posicao_Pallet.copy()
            else:
                Posicao_Palet_SP_Alterado = pd.DataFrame(columns=["Mês", "Cliente", "Ocupação", "Filial"])

            Posicao_Palet_SP_Alterado.dropna(subset="Mês", inplace=True)
            Posicao_Palet_SP_Alterado["Cliente"] = Posicao_Palet_SP_Alterado["Cliente"].str.strip()
            Posicao_Palet_SP_Alterado["Cliente"] = [re.sub(r"\s+", " ", str(cliente)).strip() for cliente in Posicao_Palet_SP_Alterado["Cliente"]]
            
            filtro = ~Posicao_Palet_SP_Alterado["Cliente"].str.contains("level", case=False, na=False, regex=True)
            Consolidar_SP = Posicao_Palet_SP_Alterado.loc[(Posicao_Palet_SP_Alterado["Mês"] != 0) & filtro].copy()
            
            Consolidar_SP["Mês"] = pd.to_datetime(Consolidar_SP["Mês"], format="mixed", errors="coerce")
            Consolidar_SP["Ano"] = Consolidar_SP["Mês"].dt.year
            Consolidar_SP["Mês"] = Consolidar_SP["Mês"].dt.month
            
            df_final_ocupacao = pd.concat([df_final_ocupacao, Consolidar_SP], ignore_index=True)
                
        df = df_final_ocupacao.copy()
        
        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Ocupacao_Armazem (processado)")
        df = df[cols]

        tamanho_antes = len(df)
        df = df.merge(
            self.mapeamentos["De_Para_Grupos_Ocupacao"],
            how="left", left_on=["Cliente", "Filial"], right_on=["cliente", "filial"]
        )
        if len(df) != tamanho_antes:
            self.alertas_tamanho.append(f"De_Para_Grupos_Ocupacao (em Ocupação): {tamanho_antes} linhas antes vs {len(df)} depois.")
        if df["grupo"].isna().any():
            self.nas_de_para_rateio.append(df[df["grupo"].isna()])
            print(f"AVISO: {df['grupo'].isna().sum()} linhas em Ocupação não encontraram 'Grupo' no De-Para.")

        df["Tabela"] = "Ocupação Armazenagem"
        df = df.rename(columns={
            "Ocupação": "saldo", "Filial": "Filial UF", 
            "area": "Area", "grupo": "Grupo", "item": "Item"
        })
        
        colunas_finais = ["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item", "saldo"]
        for col in colunas_finais:
            if col not in df.columns:
                df[col] = "N/A" 
                
        df = df[colunas_finais].loc[df["saldo"] != 0]
        print("Processando: Ocupação Armazém - Finalizado")
        return df

    def get_erros_de_para(self):
        """Retorna um DataFrame consolidado de todos os erros de De-Para."""
        if not self.nas_de_para_rateio:
            return pd.DataFrame()
        return pd.concat(self.nas_de_para_rateio, ignore_index=True).drop_duplicates()