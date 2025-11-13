import pandas as pd
import numpy as np
from .ServicoRelatoriosRateio import ServicoRelatoriosRateio

class ServicoRelatoriosDRE:
    """
    Refatoração da classe Relatorios_DRE (linhas 1238-1647 do DRE.PY).
    Replica a lógica de "estado" do st.session_state, onde um DataFrame
    principal (self.Razao_Farma_Consolidado) é modificado em etapas.
    """

    def __init__(self, mapeamentos, caminhos):
        self.mapeamentos = mapeamentos
        self.caminhos = caminhos
        
        self.nas_de_para_razao = [] 
        self.nas_classificacao_razao = [] # Lista para erros de classificação
        self.alertas_tamanho = [] 
        self.razao_para_download = pd.DataFrame()
        self.Razao_Farma_Consolidado = pd.DataFrame() 

        if not self.mapeamentos:
            raise ValueError("Mapeamentos (g.mapeamentos) não foram carregados.")
        if not self.caminhos:
            raise ValueError("Caminhos de arquivos (config.py) não foram carregados.")

    def tratar_razao(self):
        """
        Substitui Relatorios_DRE.tratar_razao().
        """
        print("Processando: Arquivo Razão (DRE)")
        cfg = self.caminhos["DRE"]
        Razao_Farma = pd.DataFrame()
        
        try:
            excel_file = pd.ExcelFile(cfg["path"])
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo DRE não encontrado em: {cfg['path']}")

        for i in cfg["sheet_name"]:
            if i not in excel_file.sheet_names:
                raise ValueError(f"Aba '{i}' não encontrada no arquivo DRE.")
            print(f"  - Lendo aba: {i}")
            Razao = pd.read_excel(excel_file, sheet_name=i, header=cfg["header"])
            Razao_Farma = pd.concat([Razao_Farma, Razao], ignore_index=True)
        
        Razao_Farma.columns = Razao_Farma.columns.astype(str).str.strip()
        if "Mês" in Razao_Farma.columns:
            Razao_Farma = Razao_Farma.drop(columns="Mês")
    
        for coluna in cfg["colunas_str"]:
            if coluna in Razao_Farma.columns:
                Razao_Farma[coluna] = Razao_Farma[coluna].astype(str).str.strip()

        Tamanho_Original = len(Razao_Farma)
        
        Razao_Farma["Data"] = pd.to_datetime(Razao_Farma["Data"], errors="coerce")
        Razao_Farma["Ano"] = Razao_Farma["Data"].dt.year.astype(str)
        Razao_Farma["Mês"] = Razao_Farma["Data"].dt.month.astype(str)

        if "Grupo" in Razao_Farma.columns:
            Razao_Farma = Razao_Farma.drop(columns=["Grupo"])

        # --- Início dos Merges de Mapeamento ---
        # 1. Centro de Custo
        Razao_Farma = Razao_Farma.merge(
            self.mapeamentos["DRE_De_Para_Centro_Custo"], 
            how="left", left_on="Centro de Custo", right_on="centro_de_custo_id"
        )
        if len(Razao_Farma) != Tamanho_Original:
            self.alertas_tamanho.append(f"DRE_De_Para_Centro_Custo (Razão): {Tamanho_Original} linhas antes vs {len(Razao_Farma)} depois.")
        if Razao_Farma["centro_custo_desc"].isna().any():
            self.nas_de_para_razao.append(Razao_Farma[Razao_Farma["centro_custo_desc"].isna()])
            print(f"AVISO: {Razao_Farma['centro_custo_desc'].isna().sum()} linhas no Razão não encontraram 'Centro de Custo' no De-Para.")

        # 2. Item Conta
        Razao_Farma = Razao_Farma.merge(
            self.mapeamentos["DRE_De_Para_Item_Conta"], 
            how="left", left_on="Item", right_on="item"
        )
        if len(Razao_Farma) != Tamanho_Original:
             self.alertas_tamanho.append(f"DRE_De_Para_Item_Conta (Razão): {Tamanho_Original} linhas antes vs {len(Razao_Farma)} depois.")
        if Razao_Farma["nome"].isna().any():
            self.nas_de_para_razao.append(Razao_Farma[Razao_Farma["nome"].isna()])
            print(f"AVISO: {Razao_Farma['nome'].isna().sum()} linhas no Razão não encontraram 'Item' no De-Para.")
            
        # 3. Filial
        Razao_Farma = Razao_Farma.merge(
            self.mapeamentos["DRE_De_Para_Filial"], 
            how="left", left_on="Filial", right_on="filial_nome"
        )
        if len(Razao_Farma) != Tamanho_Original:
            self.alertas_tamanho.append(f"DRE_De_Para_Filial (Razão): {Tamanho_Original} linhas antes vs {len(Razao_Farma)} depois.")
        if Razao_Farma["filial_uf"].isna().any():
            self.nas_de_para_razao.append(Razao_Farma[Razao_Farma["filial_uf"].isna()])
            print(f"AVISO: {Razao_Farma['filial_uf'].isna().sum()} linhas no Razão não encontraram 'Filial' no De-Para.")

        # 4. Contas Contábeis (Chave Composta)
        Razao_Farma["Concat Razão"] = (Razao_Farma["Conta"] + Razao_Farma["tipo_cc"]).astype(str).str.strip()
        
        Razao_Farma = Razao_Farma.merge(
            self.mapeamentos["DRE_De_Para_Contas_Contabeis"], 
            how='left',
            left_on='Concat Razão',
            right_on='concat_razao'
        )
        if len(Razao_Farma) != Tamanho_Original:
            self.alertas_tamanho.append(f"DRE_De_Para_Contas_Contabeis (Razão): {Tamanho_Original} linhas antes vs {len(Razao_Farma)} depois.")

        # 5. Fallback merge (lógica original)
        fallback_merge = self.mapeamentos["DRE_De_Para_Contas_Contabeis"].loc[
            self.mapeamentos["DRE_De_Para_Contas_Contabeis"]["concat_razao"].isna()
        ]
        
        sem_grupo = Razao_Farma["grupo_financeiro"].isna()
        
        df_sem_grupo = Razao_Farma[sem_grupo].drop(
            columns=["conta", "descricao_completa", "descricao_resumida", "grupo", "grupo_financeiro"], 
            errors="ignore"
        )
        
        df_sem_grupo = df_sem_grupo.merge(
            fallback_merge, 
            how="left", 
            left_on="Conta", 
            right_on="conta"
        )
        
        Razao_Farma = pd.concat([Razao_Farma[~sem_grupo], df_sem_grupo], ignore_index=True)

        # Limpa colunas de merge
        colunas_para_remover = [
            'conta_x', 'conta_y', 'conta', 
            'concat_razao_x', 'concat_razao_y', 'concat_razao',
            'item_x', 'item_y',
            'filial_nome_x', 'filial_nome_y',
            'centro_de_custo_id_x', 'centro_de_custo_id_y',
            'grupo_x', 'grupo_y', 
            'grupo_financeiro_x', 'grupo_financeiro_y'
        ]
        
        colunas_para_manter = [col for col in Razao_Farma.columns if col not in colunas_para_remover]
        colunas_para_manter.extend(['sigla', 'filial_uf', 'tipo_cc', 'centro_custo_desc', 'grupo', 'grupo_financeiro'])
        colunas_finais = list(dict.fromkeys(colunas_para_manter)) 
        
        Razao_Farma = Razao_Farma[colunas_finais]

        if Razao_Farma["grupo"].isna().any():
            self.nas_de_para_razao.append(Razao_Farma[Razao_Farma["grupo"].isna()])
            print(f"AVISO: {Razao_Farma['grupo'].isna().sum()} linhas no Razão não encontraram 'Grupo' no De-Para de Contas Contábeis (mesmo após fallback).")

        # 6. Filtro final de Depreciação
        Itens_Conta_Desconsiderar = self.mapeamentos["Item_De_Para_Filial_Depreciacao"].loc[
            self.mapeamentos["Item_De_Para_Filial_Depreciacao"]["filial_uf"] == "DESC", "item"
        ].unique()
        
        Razao_Farma = Razao_Farma.loc[~Razao_Farma["Item"].isin(Itens_Conta_Desconsiderar)]
        
        self.razao_para_download = Razao_Farma.copy() 
        self.Razao_Farma_Consolidado = Razao_Farma.copy()
        
        print("Processando: Arquivo Razão (DRE) - Finalizado")

    def Embalagem_Adequa(self):
        """
        Substitui Relatorios_DRE.Embalagem_Adequa().
        """
        print("Processando: Recorte de Embalagem, Adequação, Financeiros, Impostos...")
        
        if self.Razao_Farma_Consolidado.empty:
            raise ValueError("O Razão (tratar_razao) deve ser executado antes de Embalagem_Adequa.")

        df = self.Razao_Farma_Consolidado.copy()

        # --- Adequação ---
        mask_adeq = (df["Item"] == "10110") & (df["grupo"] == "PESSOAL OPER")
        df_adequacao = df.loc[mask_adeq].copy()
        df_adequacao["Tabela"] = "Folha Adequação"
        df_adequacao = df_adequacao.rename(columns={"Título Conta": "Area", "grupo": "Grupo", "filial_uf": "Filial UF"})
        df_adequacao = df_adequacao.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

        # --- Embalagens ---
        mask_embal = (df["Título Conta"] == "MATERIAL DE EMBALAGEM") & (~mask_adeq)
        df_embalagens = df.loc[mask_embal].copy()
        df_embalagens = df_embalagens.drop(columns={"grupo"}, errors='ignore')
        df_embalagens["Tabela"] = "MATERIAL DE EMBALAGEM"
        df_embalagens["Area"] = "Desconhecido"
        df_embalagens = df_embalagens.rename(columns={"sigla": "Grupo", "filial_uf": "Filial UF"})
        df_embalagens = df_embalagens.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

        # --- Custos Financeiros e Depreciação ---
        mask_fin = df["grupo_financeiro"].isin(["DEPREC/AMORT", "CUSTOS FINANCEIROS"]) & (~mask_adeq) & (~mask_embal)
        df_fin = df.loc[mask_fin].copy()
        df_fin = df_fin.drop(columns={"grupo"}, errors='ignore')
        df_fin = df_fin.rename(columns={"Título Conta": "Area", "grupo_financeiro": "Grupo"})
        df_fin["Tabela"] = "Custos Financeiros"

        depre = df_fin[df_fin["Grupo"] == "DEPREC/AMORT"].copy().drop(columns=["filial_uf"])
        depre["Item"] = depre["Item"].astype(str).str.strip()
        depre = depre.merge(self.mapeamentos["Item_De_Para_Filial_Depreciacao"], how='left', on='item')
        depre = depre.groupby(["Tabela", "Ano", "Mês", "filial_uf", "Grupo", "Area", "Item"], as_index=False)["saldo"].sum()
        
        outros_fin = df_fin[df_fin["Grupo"] != "DEPREC/AMORT"].copy()
        outros_fin = outros_fin.groupby(["Tabela", "Ano", "Mês", "filial_uf", "Grupo", "Area", "Item"], as_index=False)["saldo"].sum()
        
        df_custos_fin = pd.concat([outros_fin, depre], ignore_index=True)
        df_custos_fin = df_custos_fin.rename(columns={"filial_uf": "Filial UF"})

        # --- ISS ---
        mask_iss = (df["grupo"] == "ISS") & (~mask_adeq) & (~mask_embal) & (~mask_fin)
        df_iss = df.loc[mask_iss].copy()
        mapa_filial = {"10802": "GO", "10302": "SP", "10702": "RJ", "11002": "SC"}
        df_iss["filial_uf_iss"] = df_iss["Item"].map(mapa_filial)
        df_iss["filial_uf"] = df_iss["filial_uf_iss"].fillna(df_iss["filial_uf"])
        df_iss["Tabela"] = "ISS"
        df_iss["Area"] = "ISS"
        df_iss = df_iss.rename(columns={"grupo": "Grupo", "filial_uf": "Filial UF"})
        df_iss = df_iss.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

        # --- Outros Impostos ---
        mask_outros_imp = df["grupo"].isin(["PIS", "COFINS", "ICMS"]) & (~mask_adeq) & (~mask_embal) & (~mask_fin) & (~mask_iss)
        df_outros_imp = df.loc[mask_outros_imp].copy()
        df_outros_imp["Tabela"] = "Outros Impostos"
        df_outros_imp["Area"] = "Outros Impostos"
        df_outros_imp = df_outros_imp.rename(columns={"grupo": "Grupo", "filial_uf": "Filial UF"})
        df_outros_imp = df_outros_imp.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

        # --- Taxas ---
        mask_taxas = (df["grupo"] == "IMPOSTOS OPER") & (~mask_adeq) & (~mask_embal) & (~mask_fin) & (~mask_iss) & (~mask_outros_imp)
        df_taxas = df.loc[mask_taxas].copy()
        df_taxas["Tabela"] = "Custos Operacionais Indiretos - Taxas"
        df_taxas.loc[(df_taxas["centro_custo_desc"] == "Operação Armazenagem") & (df_taxas["sigla"] != "Desconhecido"), "Tabela"] = "Custos Operacionais - Taxas"
        df_taxas.loc[(df_taxas["centro_custo_desc"] != "Operação Armazenagem") & (df_taxas["sigla"] != "Desconhecido"), "Tabela"] = "Custos Operacionais Outros - Taxas"
        df_taxas = df_taxas.drop(columns={"grupo"}, errors='ignore')
        df_taxas = df_taxas.rename(columns={"sigla": "Grupo", "Título Conta": "Area", "filial_uf": "Filial UF"})
        df_taxas = df_taxas.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
        
        # --- Atualiza o estado interno (lógica destrutiva) ---
        self.Razao_Farma_Consolidado = df.loc[
            ~mask_adeq & ~mask_embal & ~mask_fin & ~mask_iss & ~mask_outros_imp & ~mask_taxas
        ].copy()
        
        print(f"Processando: Recortes 1 - Finalizado. {len(self.Razao_Farma_Consolidado)} linhas restantes no Razão.")

        # Retorna os DataFrames recortados
        return pd.concat([df_adequacao, df_embalagens, df_custos_fin, df_iss, df_outros_imp, df_taxas], ignore_index=True)

    def Overhead(self):
        """
        Substitui Relatorios_DRE.Overhead().
        """
        print("Processando: Recorte de Overhead...")
        df = self.Razao_Farma_Consolidado.copy()
        
        # --- Overhead Não Operacional ---
        mask_overhead = df["tipo_cc"] != "Oper"
        df_overhead_nao_oper = df.loc[mask_overhead].copy()
        df_overhead_nao_oper = df_overhead_nao_oper.rename(columns={"grupo": "Area", "filial_uf": "Filial UF"})
        df_overhead_nao_oper[["Tabela", "Grupo", "Item"]] = "Overhead"
        df_overhead_nao_oper = df_overhead_nao_oper[df_overhead_nao_oper["Area"] != "SERVIÇOS"]
        df_overhead_nao_oper = df_overhead_nao_oper.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
        
        # --- Indenização Trabalhista ---
        mask_indeniz = (df["Conta"] == "60301020108") & (df["tipo_cc"] == "Oper") & (~mask_overhead)
        df_indenizacao = df.loc[mask_indeniz].copy()
        df_indenizacao = df_indenizacao.rename(columns={"grupo": "Area", "filial_uf": "Filial UF"})
        df_indenizacao[["Tabela", "Grupo", "Item"]] = "Overhead"
        df_indenizacao = df_indenizacao.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
        
        # --- Atualiza o estado interno (lógica destrutiva) ---
        self.Razao_Farma_Consolidado = df.loc[~mask_overhead & ~mask_indeniz].copy()
        
        print(f"Processando: Overhead - Finalizado. {len(self.Razao_Farma_Consolidado)} linhas restantes no Razão.")
        
        return pd.concat([df_overhead_nao_oper, df_indenizacao], ignore_index=True)

    def farma_direto_indireto(self):
        """
        Substitui Relatorios_DRE.farma_direto_indireto().
        """
        print("Processando: Classificação Farma Direto/Indireto...")
        df = self.Razao_Farma_Consolidado # Modifica o DataFrame interno
        
        df["Tabela_Consolidada"] = "Tabela" # Coluna temporária
        
        cond_direto = df["sigla"] != "Desconhecido"
        cond_indireto = df["sigla"] == "Desconhecido"
        cond_armazem = df["centro_custo_desc"] == "Operação Armazenagem"
        cond_outros = df["centro_custo_desc"] != "Operação Armazenagem"

        df.loc[cond_direto & cond_armazem, "Tabela_Consolidada"] = "Farma Direto"
        df.loc[cond_direto & cond_outros, "Tabela_Consolidada"] = "Farma Direto"
        df.loc[cond_indireto & cond_armazem, "Tabela_Consolidada"] = "Farma Indireto"
        df.loc[cond_indireto & cond_outros, "Tabela_Consolidada"] = "Farma Indireto"

        print("Processando: Classificação - Finalizado.")

    def custos_alocados(self):
        """
        Substitui Relatorios_DRE.custos_alocados().
        """
        print("Processando: Custos Alocados (restante do Razão)...")
        df = self.Razao_Farma_Consolidado.copy()

        # Condições
        cond_farma_direto = df["Tabela_Consolidada"] == "Farma Direto"
        cond_farma_indireto = df["Tabela_Consolidada"] == "Farma Indireto"
        cond_pessoal_oper = df["grupo"] == "PESSOAL OPER"
        cond_oper_armazem = df["centro_custo_desc"] == "Operação Armazenagem"
        cond_outros_armazem = df["centro_custo_desc"] != "Operação Armazenagem"

        # ---
        # CORREÇÃO DO FUTUREWARNING: Inicializa a coluna como 'object'
        # ---
        df["Tabela"] = pd.Series(dtype='object') 

        # --- Folha de Pagamento ---
        df.loc[cond_farma_direto & cond_pessoal_oper & cond_oper_armazem, "Tabela"] = "Folha Razão"
        df.loc[cond_farma_direto & cond_pessoal_oper & cond_outros_armazem, "Tabela"] = "Folha Razão Outros"
        df.loc[cond_farma_indireto & cond_pessoal_oper, "Tabela"] = "Rateio Indiretos Operações"
        
        # --- Terceiros ---
        cond_terceiros_oper = df["grupo"] == "TERCEIROS OPER"
        cond_conta_temp = df["Conta"] == "60301020209"
        df.loc[cond_farma_direto & cond_terceiros_oper, "Tabela"] = "Custos Operacionais"
        df.loc[cond_farma_indireto & cond_conta_temp, "Tabela"] = "Custos Operacionais Indiretos"
        df.loc[cond_farma_direto & cond_conta_temp, "Tabela"] = "Temporarios"
        df.loc[cond_farma_indireto & cond_terceiros_oper, "Tabela"] = "Temporarios Indiretos"

        # --- Informática, Armazenagem, Outros ---
        cond_outros_grupos = df["grupo"].isin(["INFORMATICA OPER", "ARMAZENAGEM OPER", "OUTROS OPER"])
        df.loc[cond_farma_direto & cond_outros_grupos & cond_oper_armazem, "Tabela"] = "Custos Operacionais"
        df.loc[cond_farma_direto & cond_outros_grupos & cond_outros_armazem, "Tabela"] = "Custos Operacionais Outros"
        df.loc[cond_farma_indireto & cond_outros_grupos, "Tabela"] = "Custos Operacionais Indiretos"

        # --- Indenização ---
        cond_inden = df["grupo"] == "INDEN.MERCADORIAS"
        df.loc[cond_farma_direto & cond_inden & cond_oper_armazem, "Tabela"] = "Indenização de Mercadorias"

        # --- Descontos ---
        cond_descontos = df["grupo"] == "DESCONTOS"
        df.loc[cond_farma_direto & cond_descontos & cond_oper_armazem, "Tabela"] = "Descontos"

        # --- LÓGICA ANTIGA RESTAURADA ---
        df = df.rename(columns={"grupo": "Area", "sigla": "Grupo", "filial_uf": "Filial UF"})
        df["Tabela"] = df["Tabela"].fillna("Tabela Desconhecida")
        
        if "Tabela Desconhecida" in df["Tabela"].unique():
            df_desconhecido = df[df["Tabela"] == "Tabela Desconhecida"]
            if not df_desconhecido.empty:
                self.nas_classificacao_razao.append(df_desconhecido)
                print(f"AVISO: {len(df_desconhecido)} linhas serão agrupadas como 'Tabela Desconhecida'.")
        # --- FIM DA LÓGICA ANTIGA ---

        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
        
        print("Processando: Custos Alocados - Finalizado.")
        return df

    def consolidado(self, rateio_service: ServicoRelatoriosRateio):
        """
        Substitui Relatorios_DRE.consolidado().
        Orquestra todo o processo e retorna os DataFrames finais para o Excel.
        """
        # 1. Processa o Razão (DRE) em etapas
        self.tratar_razao()
        df_recortes_1 = self.Embalagem_Adequa()
        df_recortes_2 = self.Overhead()
        self.farma_direto_indireto()
        df_custos_finais = self.custos_alocados() # Processa o que sobrou

        # 2. Processa os relatórios de rateio
        df_volumes = rateio_service.carregar_volume()
        df_adequacao = rateio_service.adequacao()
        df_insumos = rateio_service.insumos()
        df_faturamento = rateio_service.faturamento()
        df_ocupacao = rateio_service.ocupacao_armazem()

        # 3. Lista de todos os DataFrames a serem consolidados
        dfs_finais = [
            df_recortes_1,
            df_recortes_2,
            df_custos_finais, # Inclui as "Tabela Desconhecida"
            df_volumes,
            df_adequacao,
            df_insumos,
            df_faturamento,
            df_ocupacao
        ]

        # 4. Concatena tudo
        resultado_final = pd.concat(dfs_finais, ignore_index=True)

        # 5. Agrupamento final
        cols_para_agrupar = ["Tabela", "Ano", "Mês", "Filial UF", "Grupo", "Area", "Item"]
        for col in cols_para_agrupar:
             if col in resultado_final.columns:
                resultado_final[col] = resultado_final[col].astype(str).replace('nan', 'N/A').replace('None', 'N/A')
             else:
                raise ValueError(f"Coluna de agrupamento '{col}' ausente no DataFrame final.")

        resultado_final = resultado_final.groupby(cols_para_agrupar, as_index=False)["saldo"].sum()
        
        print("CONSOLIDAÇÃO FINAL COMPLETA.")

        # 6. Prepara os relatórios de erro
        
        df_nas_depara_razao = pd.DataFrame()
        if self.nas_de_para_razao:
            df_nas_depara_razao = pd.concat(self.nas_de_para_razao, ignore_index=True).drop_duplicates()

        df_nas_classificacao_razao = pd.DataFrame()
        if self.nas_classificacao_razao:
            df_nas_classificacao_razao = pd.concat(self.nas_classificacao_razao, ignore_index=True).drop_duplicates()

        df_nas_depara_rateio = rateio_service.get_erros_de_para()
        
        # ---
        # CORREÇÃO DOS NOMES DAS ABAS
        # Retorna o dicionário com os nomes de aba IDÊNTICOS ao seu script antigo
        # ---
        return {
            "Rentabilidade_Armazem": resultado_final,
            "Consolidado_DRE": self.razao_para_download,
            "De_Paras_Não_Encontrados": df_nas_depara_razao,
            "De_Paras_Rateio_Não_Encontrados": df_nas_depara_rateio
            # A aba 'Erros_Classif_Razao' foi removida para 
            # corresponder 100% aos 4 arquivos originais.
        }