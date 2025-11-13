import pandas as pd
import re

class ServicoRelatoriosRateio:
    """
    Refatoração da classe Relatorios_Rateio.
    Responsável por carregar, limpar e aplicar regras de negócios (De-Para)
    nos arquivos auxiliares: Volumes, Adequação, Insumos, Faturamento e Ocupação.
    """
    
    def __init__(self, mapeamentos, caminhos):
        """
        Inicializa o serviço.
        :param mapeamentos: Dicionário com DataFrames das tabelas do banco de dados (De-Para).
        :param caminhos: Dicionário de configuração (Config.py) com caminhos dos arquivos Excel.
        """
        self.mapeamentos = mapeamentos
        self.caminhos = caminhos
        
        # Listas para acumular logs de erros e alertas durante o processamento
        self.nas_de_para_rateio = []  # Registra linhas que não encontraram correspondência no De-Para
        self.alertas_tamanho = []     # Registra se um merge duplicou linhas inesperadamente

        # Validação inicial para garantir que as dependências existem
        if not self.mapeamentos:
            raise ValueError("Mapeamentos (g.mapeamentos) não foram carregados.")
        if not self.caminhos:
            raise ValueError("Caminhos de arquivos (config.py) não foram carregados.")

    def _validar_e_renomear_colunas(self, df, colunas_esperadas, nome_arquivo):
        """
        Método auxiliar para normalizar nomes de colunas.
        O usuário pode subir um Excel com 'Data Fim', 'data fim' ou 'DATA FIM'.
        Este método garante que o código receba o nome padrão definido no Config.py.
        """
        # Cria um mapa: {NOME_MAIUSCULO_SEM_ESPACO : Nome_Original_No_Excel}
        df_cols_map = {str(col).upper().strip(): str(col) for col in df.columns}
        # Cria um mapa: {NOME_MAIUSCULO_SEM_ESPACO : Nome_Esperado_Config}
        colunas_esperadas_map = {str(col).upper().strip(): str(col) for col in colunas_esperadas}
        
        rename_map = {}
        colunas_padrao_para_retornar = []
        
        for col_upper, col_esperada in colunas_esperadas_map.items():
            # Verifica se a coluna obrigatória existe no arquivo (independente do case)
            if col_upper not in df_cols_map:
                raise ValueError(f"Coluna obrigatória '{col_esperada}' (de config.py) não encontrada no arquivo '{nome_arquivo}'. Colunas presentes: {list(df.columns)}")
            
            # Se o nome no arquivo for diferente do esperado (ex: 'data fim' vs 'Data Fim'), prepara renomeação
            nome_original_no_df = df_cols_map[col_upper]
            if nome_original_no_df != col_esperada:
                rename_map[nome_original_no_df] = col_esperada
            
            colunas_padrao_para_retornar.append(col_esperada)
        
        # Aplica a renomeação no DataFrame
        if rename_map:
            df = df.rename(columns=rename_map)
        
        # Retorna o DF com colunas corrigidas e a lista de colunas que devem ser mantidas
        return df, colunas_padrao_para_retornar

    def carregar_volume(self):
        """
        Processa o arquivo 'Volumes - Base.xlsx'.
        Objetivo: Calcular o volume de saída por Cliente/Grupo.
        """
        print("Processando: Volumes Base")
        cfg = self.caminhos["Volumes_Base"]
        
        # 1. Leitura do arquivo
        try:
            df = pd.read_excel(cfg["path"])
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de Volumes Base não encontrado em: {cfg['path']}")
        
        # 2. Validação e Renomeação de colunas
        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Volumes_Base")
        df = df[cols] # Mantém apenas as colunas necessárias
        
        # Padroniza nomes das colunas para MAIÚSCULO (facilita merges e consistência)
        df.columns = [str(c).upper().strip() for c in cols]

        # 3. Tratamento de Datas (Coluna DATAFIMPEDIDO)
        print("Volumes Base - Transformando Datas...")
        try:
            # Tenta conversão direta (dia/mês/ano)
            df["DATAFIMPEDIDO"] = pd.to_datetime(df["DATAFIMPEDIDO"], errors='coerce', dayfirst=True)
        except Exception:
            # Fallback: Se der erro, tenta limpar string (pega só a parte antes do espaço) e converter
            df["DATAFIMPEDIDO"] = pd.to_datetime(
                df["DATAFIMPEDIDO"].astype(str).str.strip().str.split(" ").str[0],
                format="mixed", dayfirst=False, errors="coerce"
            )

        # 4. Criação de Colunas Padrão para Consolidação
        df["Tabela"] = "Relatório de Saída"
        df["Mês"] = df["DATAFIMPEDIDO"].dt.month.astype(str)
        df["Ano"] = df["DATAFIMPEDIDO"].dt.year.astype(str)

        # 5. Aplicação de De-Para (Cliente -> Grupo)
        # Filtra volumes zerados antes do merge para performance
        df_filtrado = df[df["VOLUMES"] != 0].copy()
        tamanho_antes = len(df_filtrado)
        
        # Merge Left Join com a tabela de abreviação
        df = df_filtrado.merge(
            self.mapeamentos["Volumes_De_Para_Abreviacao"],
            how="left", left_on="CLIENTE", right_on="area"
        ).drop(columns="area") # Remove coluna duplicada do merge
        
        tamanho_depois = len(df)
        
        # Verifica se o merge duplicou registros (sinal de chave duplicada no De-Para)
        if tamanho_antes != tamanho_depois:
            self.alertas_tamanho.append(f"Volumes_De_Para_Abreviacao (em Volumes): {tamanho_antes} linhas antes vs {tamanho_depois} depois.")

        # 6. Renomeação Final para o padrão do DRE Consolidado
        df = df.rename(columns={
            "SITE": "Filial UF", "CATEGORIAGRUPO": "Item",
            "VOLUMES": "saldo", "CLIENTE": "Area", "grupo": "Grupo"
        })

        # 7. Log de Erros (Quem ficou sem Grupo?)
        if df["Grupo"].isna().any():
            self.nas_de_para_rateio.append(df[df["Grupo"].isna()])
            print(f"AVISO: {df['Grupo'].isna().sum()} linhas em Volumes não encontraram 'Grupo' no De-Para.")

        # 8. Agrupamento (Soma dos Saldos)
        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)[["saldo"]].sum()
        
        # Correção específica hardcoded: ITJ vira SC
        df["Filial UF"] = df["Filial UF"].str.replace("ITJ", "SC")
        
        print("Processando: Volumes Base - Finalizado")
        return df
    
    def adequacao(self):
        """
        Processa o arquivo 'Quantidade - Adequação.xlsx'.
        Objetivo: Calcular custos/volumes de serviços de adequação.
        """
        print("Processando: Adequação")
        cfg = self.caminhos["Adequacao"]

        try:
            df = pd.read_excel(cfg["path"])
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de Adequação não encontrado em: {cfg['path']}")
            
        # Validação e Padronização
        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Adequacao")
        df = df[cols]

        # Tratamento de Datas
        try:
            df["Data Fim"] = pd.to_datetime(df["Data Fim"], format="%d/%m/%Y %H:%M:%S")
        except:
            df["Data Fim"] = pd.to_datetime(
                df["Data Fim"].astype(str).str.strip().str.split(" ").str[0],
                format='mixed', dayfirst=True, errors="coerce"
            )

        # Colunas Padrão
        df["Mês"] = df["Data Fim"].dt.month.astype(str)
        df["Ano"] = df["Data Fim"].dt.year.astype(str)
        df["Tabela"] = "Relatório de Adequação"

        # De-Para (Cliente -> Grupo)
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

        # Renomeação e Agrupamento
        df = df.rename(columns={
            "Serviço": "Area", "Nome Servico": "Item", 
            "Qtde Real": "saldo", "Filial": "Filial UF", "grupo": "Grupo"
        })
        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)[["saldo"]].sum()
        
        print("Processando: Adequação - Finalizado")
        return df

    def insumos(self):
        """
        Processa o arquivo 'Insumos.xlsx'.
        Objetivo: Calcular custos de insumos cobrados.
        """
        print("Processando: Insumos")
        cfg = self.caminhos["Insumos"]
        
        try:
            df = pd.read_excel(cfg["path"])
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de Insumos não encontrado em: {cfg['path']}")

        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Insumos")
        df = df[cols].loc[df["Custo"] != 0] # Ignora custo zero

        df["Tabela"] = "Insumos"
        df["ID"] = df["ID"].astype(str)
        df["Ano"] = df["ID"].str[:4] # Extrai Ano dos primeiros 4 dígitos do ID

        # --- MERGE 1: CLIENTES (Para descobrir a Filial UF) ---
        tamanho_antes = len(df)
        df = df.merge(
            self.mapeamentos["Embalagens_De_Para_Clientes"], 
            how='left', left_on='NOMECLI', right_on='nome_cliente'
        )
        if len(df) != tamanho_antes:
            self.alertas_tamanho.append(f"Embalagens_De_Para_Clientes (em Insumos): alterou tamanho.")
            
        if df["filial_uf"].isna().any():
            self.nas_de_para_rateio.append(df[df["filial_uf"].isna()])
            print(f"AVISO: {df['filial_uf'].isna().sum()} linhas em Insumos não encontraram 'Filial UF'.")

        # --- MERGE 2: VOLUMES/ABREVIAÇÃO (Para descobrir o Grupo) ---
        tamanho_antes = len(df)
        df = df.merge(
            self.mapeamentos["Volumes_De_Para_Abreviacao"], 
            how='left', left_on='Depositante', right_on='area'
        ).drop(columns="area")
        
        if len(df) != tamanho_antes:
            self.alertas_tamanho.append(f"Volumes_De_Para_Abreviacao (em Insumos): alterou tamanho.")

        if df["grupo"].isna().any():
            self.nas_de_para_rateio.append(df[df["grupo"].isna()])
            print(f"AVISO: {df['grupo'].isna().sum()} linhas em Insumos não encontraram 'Grupo'.")

        # Renomeação e Agrupamento
        df = df.rename(columns={
            "NOMECLI": "Area", "Insumo": "Item", "Custo": "saldo", 
            "filial_uf": "Filial UF", "grupo": "Grupo"
        })
        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)[["saldo"]].sum()
        
        # REGRA DE NEGÓCIO: Fator de redução (provavelmente impostos/margem)
        df["saldo"] = df["saldo"] * 0.9075
        
        print("Processando: Insumos - Finalizado")
        return df

    def faturamento(self):
        """
        Processa o arquivo 'Faturamento 2025.xlsx'.
        """
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
            raise ValueError(f"Erro ao ler aba '{cfg['sheet_name']}': {e}")

        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Faturamento")
        
        # FILTROS RÍGIDOS (definidos no Config.py)
        # Ex: Empresa=FARMA, Ano=2025, Versão=Real, Receita=Serviços
        df = df[df["EMPRESA"].isin(cfg["empresa"]) &
                df["ANO"].isin(cfg["ano"]) &
                df["VERSÃO"].isin(cfg["versao"]) &
                df["RECEITA"].isin(cfg["receita"]) &
                df["VALOR R$"].notna()]
        
        # Pré-agrupamento
        df = df.groupby(["ANO", "MÊS", "EMPRESA", "FILIAL", "CLIENTE", "TIPO"], as_index=False)[["VALOR R$"]].sum()
        
        # Aplicação do fator redutor (0.9075)
        df["VALOR R$"] = df["VALOR R$"] * 0.9075
        
        # Limpeza da coluna Ano (remove decimais ex: 2025.0 -> 2025)
        df["ANO"] = df["ANO"].astype(str).str.split(r'[. ]').str[0]

        # De-Para (Filial Nome -> Filial UF)
        tamanho_antes = len(df)
        df = df.merge(
            self.mapeamentos["DRE_De_Para_Filial"], 
            how='left', left_on="FILIAL", right_on="filial_nome"
        )
        if len(df) != tamanho_antes:
             self.alertas_tamanho.append(f"DRE_De_Para_Filial (em Faturamento): alterou tamanho.")
        
        if df["filial_uf"].isna().any():
            self.nas_de_para_rateio.append(df[df["filial_uf"].isna()])
            print(f"AVISO: {df['filial_uf'].isna().sum()} linhas em Faturamento não encontraram 'Filial UF'.")

        # Padronização Final
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
        """
        Processa o arquivo 'Acompanhamento Pallets'.
        Lógica COMPLEXA devido ao formato 'pivotado' e multi-header do Excel original.
        As colunas são datas dinâmicas e há colunas de totais que precisam ser ignoradas.
        """
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
            
            # Lê com MultiIndex (Header nas linhas 4 e 5)
            Ocupacao_Armazem = pd.read_excel(excel_file, sheet_name=i, header=cfg["header"])
            month = cfg["escrita_mes"] # String usada para identificar colunas de mês
            
            Posicao_Palet_SP_Alterado = Ocupacao_Armazem.copy().fillna(0)
            
            # --- Limpeza: Remove colunas onde a soma é 0 ---
            colunas_para_remover = []
            for col in Posicao_Palet_SP_Alterado.columns:
                try:
                    if pd.api.types.is_numeric_dtype(Posicao_Palet_SP_Alterado[col]):
                        if Posicao_Palet_SP_Alterado[col].sum() == 0:
                            colunas_para_remover.append(col)
                except (TypeError, ValueError):
                    pass
            Posicao_Palet_SP_Alterado = Posicao_Palet_SP_Alterado.drop(columns=colunas_para_remover)

            # --- Normalização dos Nomes de Clientes no Header ---
            Clientes_2 = []
            for column in Posicao_Palet_SP_Alterado.columns:
                # Trata tuplas do MultiIndex ou strings simples
                novo_nome = column[0].replace("  ", " ").strip() if isinstance(column, tuple) else column.replace("  ", " ").strip()
                Clientes_2.append(novo_nome)
            
            # Separa o DataFrame em duas partes: Dados detalhados e Totais
            Colunas = Posicao_Palet_SP_Alterado.columns
            Registros_Totais = pd.DataFrame()
            Posicao_Pallet = pd.DataFrame()

            for outro in Colunas:
                df_subset = Posicao_Palet_SP_Alterado[outro[0]]
                # Lógica para decidir se coluna vai para 'Registros_Totais' ou 'Posicao_Pallet'
                if isinstance(df_subset, pd.DataFrame):
                    if len(df_subset.columns.values) > 1:
                        Registros_Totais[outro] = Posicao_Palet_SP_Alterado[outro]
                    else:
                        Posicao_Pallet[outro] = Posicao_Palet_SP_Alterado[outro]
                else:
                    Posicao_Pallet[outro] = Posicao_Palet_SP_Alterado[outro]

            # --- Processamento da parte 'Posicao_Pallet' (Unpivot/Stack) ---
            if not Posicao_Pallet.empty:
                renomear = []
                for column in Posicao_Pallet.columns.values:
                    if isinstance(column, tuple):
                        name1, name2 = column
                        renomear.append(name2 if name2 == "Mês" else name1)
                    else:
                        renomear.append(column) 
                Posicao_Pallet.columns = renomear

                # Transforma Colunas em Linhas (Melt/Stack)
                Posicao_Pallet = (Posicao_Pallet.set_index(["Mês"])
                                        .stack()
                                        .reset_index(name='Ocupação')
                                        .rename(columns={'level_1':'Cliente'}))
                Posicao_Pallet["Filial"] = i

            # --- Processamento da parte 'Registros_Totais' ---
            if not Registros_Totais.empty:
                # Normaliza headers para conseguir filtrar 'TOTAL'
                Registros_Totais.columns = Registros_Totais.columns.map(lambda x: str(x).upper())
                
                # Separa o que NÃO é TOTAL (Clientes Sem Total)
                Clientes_Sem_Total = Registros_Totais.loc[:, ~Registros_Totais.columns.str.contains("TOTAL", case=False, na=False)]
                if not Clientes_Sem_Total.empty:
                    # Agrupa duplicatas
                    Clientes_Sem_Total = Clientes_Sem_Total.T.groupby(level=0).sum().T
                
                # Separa o que É TOTAL
                Registros_Totais = Registros_Totais.loc[:, Registros_Totais.columns.str.contains("TOTAL", case=False, na=False)]
                # Limpa caracteres indesejados dos nomes
                Registros_Totais.columns = [str(col).replace("(", "").replace(")", "").replace("'", "") for col in Registros_Totais.columns]
                Registros_Totais.columns = [tuple(col.split(',')) for col in Registros_Totais.columns]
                
                renomear = [str(col[0]).strip() for col in Registros_Totais.columns]
                Registros_Totais.columns = renomear

                # Reintegra Clientes Sem Total se houver
                if not Clientes_Sem_Total.empty:
                    for col_total in Registros_Totais.columns.values:
                        Clientes_Sem_Total = Clientes_Sem_Total.loc[:, ~Clientes_Sem_Total.columns.str.contains(col_total, case=False, na=False)]
                    
                    # ... (Mais limpeza de string) ...
                    Clientes_Sem_Total.columns = [str(col).replace("(", "").replace(")", "").replace("'", "") for col in Clientes_Sem_Total.columns]
                    Clientes_Sem_Total.columns = [tuple(col.split(',')) for col in Clientes_Sem_Total.columns]
                    renomear = [str(col[0]).strip() for col in Clientes_Sem_Total.columns]
                    Clientes_Sem_Total.columns = renomear
                    
                    Clientes_Sem_Total = Clientes_Sem_Total.T.groupby(level=0).sum().T
                    Registros_Totais = pd.concat([Registros_Totais, Clientes_Sem_Total], axis=1)

                # Tenta recuperar a coluna 'Mês' do outro dataframe se faltar aqui
                if not Posicao_Pallet.empty and "Mês" in Posicao_Pallet.columns:
                     Registros_Totais["Mês"] = list(Posicao_Pallet["Mês"].unique())
                else:
                     # Fallback
                     col_mes_fallback = [col for col in Posicao_Palet_SP_Alterado.columns if 'Mês' in str(col[0]) or 'Mês' in str(col[1])]
                     if col_mes_fallback:
                         Registros_Totais["Mês"] = Posicao_Palet_SP_Alterado[col_mes_fallback[0]].values
                     else:
                         raise ValueError("Não foi possível encontrar a coluna 'Mês' para o pivot.")

                # Unpivot/Stack dos Totais
                Registros_Totais = (Registros_Totais.set_index(["Mês"])
                                        .stack()
                                        .reset_index(name='Ocupação')
                                        .rename(columns={'level_1':'Cliente'}))
                Registros_Totais["Filial"] = i

            # Concatena as duas partes processadas (Normal + Totais)
            if not Posicao_Pallet.empty and not Registros_Totais.empty:
                Posicao_Palet_SP_Alterado = pd.concat([Posicao_Pallet, Registros_Totais])
            elif Posicao_Pallet.empty and not Registros_Totais.empty:
                Posicao_Palet_SP_Alterado = Registros_Totais.copy()
            elif not Posicao_Pallet.empty and Registros_Totais.empty:
                Posicao_Palet_SP_Alterado = Posicao_Pallet.copy()
            else:
                Posicao_Palet_SP_Alterado = pd.DataFrame(columns=["Mês", "Cliente", "Ocupação", "Filial"])

            # Limpeza Final dos Dados Empilhados
            Posicao_Palet_SP_Alterado.dropna(subset="Mês", inplace=True)
            Posicao_Palet_SP_Alterado["Cliente"] = Posicao_Palet_SP_Alterado["Cliente"].str.strip()
            # Regex para limpar espaços múltiplos
            Posicao_Palet_SP_Alterado["Cliente"] = [re.sub(r"\s+", " ", str(cliente)).strip() for cliente in Posicao_Palet_SP_Alterado["Cliente"]]
            
            # Remove linhas de lixo (índices do Pandas 'level')
            filtro = ~Posicao_Palet_SP_Alterado["Cliente"].str.contains("level", case=False, na=False, regex=True)
            Consolidar_SP = Posicao_Palet_SP_Alterado.loc[(Posicao_Palet_SP_Alterado["Mês"] != 0) & filtro].copy()
            
            # Extração de Data
            Consolidar_SP["Mês"] = pd.to_datetime(Consolidar_SP["Mês"], format="mixed", errors="coerce")
            Consolidar_SP["Ano"] = Consolidar_SP["Mês"].dt.year
            Consolidar_SP["Mês"] = Consolidar_SP["Mês"].dt.month
            
            # Adiciona ao acumulador geral
            df_final_ocupacao = pd.concat([df_final_ocupacao, Consolidar_SP], ignore_index=True)
                
        df = df_final_ocupacao.copy()
        
        # Padronização de Colunas Finais
        df, cols = self._validar_e_renomear_colunas(df, cfg["columns"], "Ocupacao_Armazem (processado)")
        df = df[cols]

        # De-Para (Cliente/Filial -> Grupo)
        tamanho_antes = len(df)
        df = df.merge(
            self.mapeamentos["De_Para_Grupos_Ocupacao"],
            how="left", left_on=["Cliente", "Filial"], right_on=["cliente", "filial"]
        )
        if len(df) != tamanho_antes:
            self.alertas_tamanho.append(f"De_Para_Grupos_Ocupacao (em Ocupação): alterou tamanho.")
        if df["grupo"].isna().any():
            self.nas_de_para_rateio.append(df[df["grupo"].isna()])
            print(f"AVISO: {df['grupo'].isna().sum()} linhas em Ocupação não encontraram 'Grupo' no De-Para.")

        # Formatação Final
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
        """
        Retorna um DataFrame consolidado de todos os erros de De-Para encontrados
        durante o processamento dos 5 arquivos.
        """
        if not self.nas_de_para_rateio:
            return pd.DataFrame()
        return pd.concat(self.nas_de_para_rateio, ignore_index=True).drop_duplicates()