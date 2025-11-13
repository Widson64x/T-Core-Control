import pandas as pd
import numpy as np
from .ServicoRelatoriosRateio import ServicoRelatoriosRateio

class ServicoRelatoriosDRE:
    """
    Refatoração da classe Relatorios_DRE.
    Gerencia o processamento do Razão Contábil (DRE).
    Mantém um 'estado' interno (self.Razao_Farma_Consolidado) que é modificado
    destrutivamente conforme os métodos de recorte são chamados.
    """

    def __init__(self, mapeamentos, caminhos):
        self.mapeamentos = mapeamentos
        self.caminhos = caminhos
        
        # Logs de erro
        self.nas_de_para_razao = [] # Itens do Razão que não acharam De-Para
        self.nas_classificacao_razao = [] # Erros de lógica de classificação final
        self.alertas_tamanho = [] 
        
        self.razao_para_download = pd.DataFrame() # Cópia do DRE completo tratado
        self.Razao_Farma_Consolidado = pd.DataFrame() # DRE de trabalho (será recortado)

        if not self.mapeamentos:
            raise ValueError("Mapeamentos (g.mapeamentos) não foram carregados.")
        if not self.caminhos:
            raise ValueError("Caminhos de arquivos (config.py) não foram carregados.")

    def tratar_razao(self):
        """
        ETAPA 1: CARGA E ENRIQUECIMENTO.
        Lê o arquivo Excel do DRE, padroniza tipos e aplica os 4 De-Paras principais:
        1. Centro de Custo
        2. Item
        3. Filial
        4. Contas Contábeis (com lógica de Fallback)
        """
        print("Processando: Arquivo Razão (DRE)")
        cfg = self.caminhos["DRE"]
        Razao_Farma = pd.DataFrame()
        
        # 1. Leitura do Excel (pode ter múltiplas abas)
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
        
        # 2. Limpeza de Colunas
        Razao_Farma.columns = Razao_Farma.columns.astype(str).str.strip()
        if "Mês" in Razao_Farma.columns:
            Razao_Farma = Razao_Farma.drop(columns="Mês") # Recalcula mês via Data
    
        # Converte colunas chave para String para garantir merge correto
        for coluna in cfg["colunas_str"]:
            if coluna in Razao_Farma.columns:
                Razao_Farma[coluna] = Razao_Farma[coluna].astype(str).str.strip()

        Tamanho_Original = len(Razao_Farma)
        
        # 3. Conversão de Datas
        Razao_Farma["Data"] = pd.to_datetime(Razao_Farma["Data"], errors="coerce")
        Razao_Farma["Ano"] = Razao_Farma["Data"].dt.year.astype(str)
        Razao_Farma["Mês"] = Razao_Farma["Data"].dt.month.astype(str)

        # Remove coluna 'Grupo' se já vier no excel, pois vamos trazer do De-Para
        if "Grupo" in Razao_Farma.columns:
            Razao_Farma = Razao_Farma.drop(columns=["Grupo"])

        # --- INÍCIO DOS MERGES (ENRIQUECIMENTO) ---
        
        # 4. Merge: Centro de Custo
        Razao_Farma = Razao_Farma.merge(
            self.mapeamentos["DRE_De_Para_Centro_Custo"], 
            how="left", left_on="Centro de Custo", right_on="centro_de_custo_id"
        )
        # Verifica duplicidade
        if len(Razao_Farma) != Tamanho_Original:
            self.alertas_tamanho.append(f"DRE_De_Para_Centro_Custo (Razão): alterou tamanho.")
        # Loga erros
        if Razao_Farma["centro_custo_desc"].isna().any():
            self.nas_de_para_razao.append(Razao_Farma[Razao_Farma["centro_custo_desc"].isna()])
            print(f"AVISO: {Razao_Farma['centro_custo_desc'].isna().sum()} linhas sem Centro de Custo.")

        # 5. Merge: Item Conta
        Razao_Farma = Razao_Farma.merge(
            self.mapeamentos["DRE_De_Para_Item_Conta"], 
            how="left", left_on="Item", right_on="item"
        )
        if len(Razao_Farma) != Tamanho_Original:
             self.alertas_tamanho.append(f"DRE_De_Para_Item_Conta (Razão): alterou tamanho.")
        if Razao_Farma["nome"].isna().any():
            self.nas_de_para_razao.append(Razao_Farma[Razao_Farma["nome"].isna()])
            print(f"AVISO: {Razao_Farma['nome'].isna().sum()} linhas sem Item.")
            
        # 6. Merge: Filial
        Razao_Farma = Razao_Farma.merge(
            self.mapeamentos["DRE_De_Para_Filial"], 
            how="left", left_on="Filial", right_on="filial_nome"
        )
        if len(Razao_Farma) != Tamanho_Original:
            self.alertas_tamanho.append(f"DRE_De_Para_Filial (Razão): alterou tamanho.")
        if Razao_Farma["filial_uf"].isna().any():
            self.nas_de_para_razao.append(Razao_Farma[Razao_Farma["filial_uf"].isna()])
            print(f"AVISO: {Razao_Farma['filial_uf'].isna().sum()} linhas sem Filial.")

        # 7. Merge: Contas Contábeis (Lógica de Dupla Tentativa)
        # Tentativa 1: Chave Composta (Conta + TipoCC)
        Razao_Farma["Concat Razão"] = (Razao_Farma["Conta"] + Razao_Farma["tipo_cc"]).astype(str).str.strip()
        
        Razao_Farma = Razao_Farma.merge(
            self.mapeamentos["DRE_De_Para_Contas_Contabeis"], 
            how='left',
            left_on='Concat Razão',
            right_on='concat_razao'
        )
        if len(Razao_Farma) != Tamanho_Original:
            self.alertas_tamanho.append(f"DRE_De_Para_Contas_Contabeis (Razão): alterou tamanho.")

        # Tentativa 2 (Fallback): Para quem não casou na chave composta, tenta só pela Conta.
        # Pega o pedaço do De-Para que tem chave composta vazia (regra genérica)
        fallback_merge = self.mapeamentos["DRE_De_Para_Contas_Contabeis"].loc[
            self.mapeamentos["DRE_De_Para_Contas_Contabeis"]["concat_razao"].isna()
        ]
        
        # Identifica linhas do Razão que falharam no primeiro merge (grupo_financeiro é vazio)
        sem_grupo = Razao_Farma["grupo_financeiro"].isna()
        
        # Separa e limpa colunas vazias do merge falho
        df_sem_grupo = Razao_Farma[sem_grupo].drop(
            columns=["conta", "descricao_completa", "descricao_resumida", "grupo", "grupo_financeiro"], 
            errors="ignore"
        )
        
        # Reaplica merge só com a chave 'Conta'
        df_sem_grupo = df_sem_grupo.merge(
            fallback_merge, 
            how="left", 
            left_on="Conta", 
            right_on="conta"
        )
        
        # Reintegra os dados (Sucesso da Tentativa 1 + Sucesso da Tentativa 2)
        Razao_Farma = pd.concat([Razao_Farma[~sem_grupo], df_sem_grupo], ignore_index=True)

        # 8. Limpeza Pós-Merge (Remove colunas sujas _x, _y)
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
        # Garante que as colunas essenciais fiquem
        colunas_para_manter.extend(['sigla', 'filial_uf', 'tipo_cc', 'centro_custo_desc', 'grupo', 'grupo_financeiro'])
        colunas_finais = list(dict.fromkeys(colunas_para_manter)) # Remove duplicatas mantendo ordem
        
        Razao_Farma = Razao_Farma[colunas_finais]

        if Razao_Farma["grupo"].isna().any():
            self.nas_de_para_razao.append(Razao_Farma[Razao_Farma["grupo"].isna()])
            print(f"AVISO: {Razao_Farma['grupo'].isna().sum()} linhas sem Grupo Contábil.")

        # 9. Filtro de Itens de Depreciação (Regra específica por UF)
        Itens_Conta_Desconsiderar = self.mapeamentos["Item_De_Para_Filial_Depreciacao"].loc[
            self.mapeamentos["Item_De_Para_Filial_Depreciacao"]["filial_uf"] == "DESC", "item"
        ].unique()
        
        Razao_Farma = Razao_Farma.loc[~Razao_Farma["Item"].isin(Itens_Conta_Desconsiderar)]
        
        # Salva o estado para uso nos próximos métodos
        self.razao_para_download = Razao_Farma.copy() 
        self.Razao_Farma_Consolidado = Razao_Farma.copy()
        
        print("Processando: Arquivo Razão (DRE) - Finalizado")

    def Embalagem_Adequa(self):
        """
        ETAPA 2: RECORTE.
        Identifica e remove do Razão linhas referentes a:
        - Adequação (Folha)
        - Embalagens
        - Custos Financeiros / Depreciação
        - Impostos (ISS, PIS, COFINS, etc)
        - Taxas
        
        Retorna: Um DataFrame com todos esses itens formatados.
        Efeito Colateral: Remove esses itens de self.Razao_Farma_Consolidado.
        """
        print("Processando: Recorte de Embalagem, Adequação, Financeiros, Impostos...")
        
        if self.Razao_Farma_Consolidado.empty:
            raise ValueError("O Razão (tratar_razao) deve ser executado antes de Embalagem_Adequa.")

        df = self.Razao_Farma_Consolidado.copy()

        # --- Adequação ---
        # Regra: Item 10110 + Grupo PESSOAL OPER
        mask_adeq = (df["Item"] == "10110") & (df["grupo"] == "PESSOAL OPER")
        df_adequacao = df.loc[mask_adeq].copy()
        df_adequacao["Tabela"] = "Folha Adequação"
        df_adequacao = df_adequacao.rename(columns={"Título Conta": "Area", "grupo": "Grupo", "filial_uf": "Filial UF"})
        df_adequacao = df_adequacao.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

        # --- Embalagens ---
        # Regra: Título Conta 'MATERIAL DE EMBALAGEM' (exceto se for adequação)
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

        # Separa Depreciação para aplicar De-Para extra (Item -> Filial)
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
        # Regra manual de De-Para para ISS (Item -> UF)
        mapa_filial = {"10802": "GO", "10302": "SP", "10702": "RJ", "11002": "SC"}
        df_iss["filial_uf_iss"] = df_iss["Item"].map(mapa_filial)
        df_iss["filial_uf"] = df_iss["filial_uf_iss"].fillna(df_iss["filial_uf"])
        df_iss["Tabela"] = "ISS"
        df_iss["Area"] = "ISS"
        df_iss = df_iss.rename(columns={"grupo": "Grupo", "filial_uf": "Filial UF"})
        df_iss = df_iss.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

        # --- Outros Impostos (PIS, COFINS, ICMS) ---
        mask_outros_imp = df["grupo"].isin(["PIS", "COFINS", "ICMS"]) & (~mask_adeq) & (~mask_embal) & (~mask_fin) & (~mask_iss)
        df_outros_imp = df.loc[mask_outros_imp].copy()
        df_outros_imp["Tabela"] = "Outros Impostos"
        df_outros_imp["Area"] = "Outros Impostos"
        df_outros_imp = df_outros_imp.rename(columns={"grupo": "Grupo", "filial_uf": "Filial UF"})
        df_outros_imp = df_outros_imp.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

        # --- Taxas Operacionais ---
        mask_taxas = (df["grupo"] == "IMPOSTOS OPER") & (~mask_adeq) & (~mask_embal) & (~mask_fin) & (~mask_iss) & (~mask_outros_imp)
        df_taxas = df.loc[mask_taxas].copy()
        # Classifica nome da tabela baseado no Centro de Custo e Sigla
        df_taxas["Tabela"] = "Custos Operacionais Indiretos - Taxas"
        df_taxas.loc[(df_taxas["centro_custo_desc"] == "Operação Armazenagem") & (df_taxas["sigla"] != "Desconhecido"), "Tabela"] = "Custos Operacionais - Taxas"
        df_taxas.loc[(df_taxas["centro_custo_desc"] != "Operação Armazenagem") & (df_taxas["sigla"] != "Desconhecido"), "Tabela"] = "Custos Operacionais Outros - Taxas"
        
        df_taxas = df_taxas.drop(columns={"grupo"}, errors='ignore')
        df_taxas = df_taxas.rename(columns={"sigla": "Grupo", "Título Conta": "Area", "filial_uf": "Filial UF"})
        df_taxas = df_taxas.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
        
        # --- LÓGICA DESTRUTIVA: Atualiza o DataFrame principal ---
        # Remove tudo que foi processado acima
        self.Razao_Farma_Consolidado = df.loc[
            ~mask_adeq & ~mask_embal & ~mask_fin & ~mask_iss & ~mask_outros_imp & ~mask_taxas
        ].copy()
        
        print(f"Processando: Recortes 1 - Finalizado. {len(self.Razao_Farma_Consolidado)} linhas restantes no Razão.")

        # Retorna a união dos recortes
        return pd.concat([df_adequacao, df_embalagens, df_custos_fin, df_iss, df_outros_imp, df_taxas], ignore_index=True)

    def Overhead(self):
        """
        ETAPA 3: RECORTE DE OVERHEAD.
        Separa custos não operacionais (Overhead) e Indenizações.
        """
        print("Processando: Recorte de Overhead...")
        df = self.Razao_Farma_Consolidado.copy()
        
        # --- Overhead Não Operacional ---
        # Regra: Tudo que não tiver TipoCC = "Oper"
        mask_overhead = df["tipo_cc"] != "Oper"
        df_overhead_nao_oper = df.loc[mask_overhead].copy()
        df_overhead_nao_oper = df_overhead_nao_oper.rename(columns={"grupo": "Area", "filial_uf": "Filial UF"})
        df_overhead_nao_oper[["Tabela", "Grupo", "Item"]] = "Overhead"
        # Remove serviços (exceção específica)
        df_overhead_nao_oper = df_overhead_nao_oper[df_overhead_nao_oper["Area"] != "SERVIÇOS"]
        df_overhead_nao_oper = df_overhead_nao_oper.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
        
        # --- Indenização Trabalhista ---
        # Regra: Conta específica '60301020108' que seja operacional
        mask_indeniz = (df["Conta"] == "60301020108") & (df["tipo_cc"] == "Oper") & (~mask_overhead)
        df_indenizacao = df.loc[mask_indeniz].copy()
        df_indenizacao = df_indenizacao.rename(columns={"grupo": "Area", "filial_uf": "Filial UF"})
        df_indenizacao[["Tabela", "Grupo", "Item"]] = "Overhead"
        df_indenizacao = df_indenizacao.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
        
        # --- LÓGICA DESTRUTIVA ---
        self.Razao_Farma_Consolidado = df.loc[~mask_overhead & ~mask_indeniz].copy()
        
        print(f"Processando: Overhead - Finalizado. {len(self.Razao_Farma_Consolidado)} linhas restantes no Razão.")
        
        return pd.concat([df_overhead_nao_oper, df_indenizacao], ignore_index=True)

    def farma_direto_indireto(self):
        """
        ETAPA 4: CLASSIFICAÇÃO INTERMÉDIA.
        Marca nas linhas restantes se são 'Farma Direto' ou 'Farma Indireto'
        baseado na coluna 'sigla' e 'centro_custo_desc'.
        Esta marcação é temporária para uso no método 'custos_alocados'.
        """
        print("Processando: Classificação Farma Direto/Indireto...")
        df = self.Razao_Farma_Consolidado # Modifica a referência direta
        
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
        ETAPA 5: CUSTOS OPERACIONAIS FINAIS.
        Classifica o que sobrou do Razão em tabelas finais:
        - Folha Razão
        - Custos Operacionais
        - Temporários
        - Indenizações / Descontos
        """
        print("Processando: Custos Alocados (restante do Razão)...")
        df = self.Razao_Farma_Consolidado.copy()

        # Definição das máscaras booleanas para facilitar leitura
        cond_farma_direto = df["Tabela_Consolidada"] == "Farma Direto"
        cond_farma_indireto = df["Tabela_Consolidada"] == "Farma Indireto"
        cond_pessoal_oper = df["grupo"] == "PESSOAL OPER"
        cond_oper_armazem = df["centro_custo_desc"] == "Operação Armazenagem"
        cond_outros_armazem = df["centro_custo_desc"] != "Operação Armazenagem"

        # Inicializa coluna Tabela para evitar warnings
        df["Tabela"] = pd.Series(dtype='object') 

        # --- Folha de Pagamento ---
        df.loc[cond_farma_direto & cond_pessoal_oper & cond_oper_armazem, "Tabela"] = "Folha Razão"
        df.loc[cond_farma_direto & cond_pessoal_oper & cond_outros_armazem, "Tabela"] = "Folha Razão Outros"
        df.loc[cond_farma_indireto & cond_pessoal_oper, "Tabela"] = "Rateio Indiretos Operações"
        
        # --- Terceiros ---
        cond_terceiros_oper = df["grupo"] == "TERCEIROS OPER"
        cond_conta_temp = df["Conta"] == "60301020209" # Conta específica de temporários
        df.loc[cond_farma_direto & cond_terceiros_oper, "Tabela"] = "Custos Operacionais"
        df.loc[cond_farma_indireto & cond_conta_temp, "Tabela"] = "Custos Operacionais Indiretos"
        df.loc[cond_farma_direto & cond_conta_temp, "Tabela"] = "Temporarios"
        df.loc[cond_farma_indireto & cond_terceiros_oper, "Tabela"] = "Temporarios Indiretos"

        # --- Informática, Armazenagem, Outros ---
        cond_outros_grupos = df["grupo"].isin(["INFORMATICA OPER", "ARMAZENAGEM OPER", "OUTROS OPER"])
        df.loc[cond_farma_direto & cond_outros_grupos & cond_oper_armazem, "Tabela"] = "Custos Operacionais"
        df.loc[cond_farma_direto & cond_outros_grupos & cond_outros_armazem, "Tabela"] = "Custos Operacionais Outros"
        df.loc[cond_farma_indireto & cond_outros_grupos, "Tabela"] = "Custos Operacionais Indiretos"

        # --- Indenização / Descontos ---
        cond_inden = df["grupo"] == "INDEN.MERCADORIAS"
        df.loc[cond_farma_direto & cond_inden & cond_oper_armazem, "Tabela"] = "Indenização de Mercadorias"

        cond_descontos = df["grupo"] == "DESCONTOS"
        df.loc[cond_farma_direto & cond_descontos & cond_oper_armazem, "Tabela"] = "Descontos"

        # --- Tratamento de Não Classificados ---
        df = df.rename(columns={"grupo": "Area", "sigla": "Grupo", "filial_uf": "Filial UF"})
        df["Tabela"] = df["Tabela"].fillna("Tabela Desconhecida")
        
        if "Tabela Desconhecida" in df["Tabela"].unique():
            df_desconhecido = df[df["Tabela"] == "Tabela Desconhecida"]
            if not df_desconhecido.empty:
                self.nas_classificacao_razao.append(df_desconhecido)
                print(f"AVISO: {len(df_desconhecido)} linhas não caíram em nenhuma regra e viraram 'Tabela Desconhecida'.")

        # Agrupamento Final
        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
        
        print("Processando: Custos Alocados - Finalizado.")
        return df

    def consolidado(self, rateio_service: ServicoRelatoriosRateio):
        """
        ETAPA FINAL: ORQUESTRAÇÃO.
        Chama todos os métodos na ordem correta, coleta os dataframes de Rateio
        e concatena tudo em um único resultado.
        """
        # 1. Processa o Razão (DRE) em etapas (Destrutivo)
        self.tratar_razao()
        df_recortes_1 = self.Embalagem_Adequa()
        df_recortes_2 = self.Overhead()
        self.farma_direto_indireto()
        df_custos_finais = self.custos_alocados() # Processa o que sobrou

        # 2. Processa os relatórios externos (via serviço injetado)
        df_volumes = rateio_service.carregar_volume()
        df_adequacao = rateio_service.adequacao()
        df_insumos = rateio_service.insumos()
        df_faturamento = rateio_service.faturamento()
        df_ocupacao = rateio_service.ocupacao_armazem()

        # 3. Lista de todos os DataFrames a serem consolidados
        dfs_finais = [
            df_recortes_1,
            df_recortes_2,
            df_custos_finais,
            df_volumes,
            df_adequacao,
            df_insumos,
            df_faturamento,
            df_ocupacao
        ]

        # 4. Concatena tudo (Union)
        resultado_final = pd.concat(dfs_finais, ignore_index=True)

        # 5. Tratamento final de Nulos e Agrupamento
        cols_para_agrupar = ["Tabela", "Ano", "Mês", "Filial UF", "Grupo", "Area", "Item"]
        for col in cols_para_agrupar:
             if col in resultado_final.columns:
                resultado_final[col] = resultado_final[col].astype(str).replace('nan', 'N/A').replace('None', 'N/A')
             else:
                raise ValueError(f"Coluna de agrupamento '{col}' ausente no DataFrame final.")

        resultado_final = resultado_final.groupby(cols_para_agrupar, as_index=False)["saldo"].sum()
        
        print("CONSOLIDAÇÃO FINAL COMPLETA.")

        # 6. Prepara os relatórios de erro (para abas extras no Excel)
        df_nas_depara_razao = pd.DataFrame()
        if self.nas_de_para_razao:
            df_nas_depara_razao = pd.concat(self.nas_de_para_razao, ignore_index=True).drop_duplicates()

        # (Opcional) df_nas_classificacao_razao poderia ser exportado aqui também
        # df_nas_classificacao_razao = pd.concat(self.nas_classificacao_razao, ...)

        df_nas_depara_rateio = rateio_service.get_erros_de_para()
        
        # Retorna dicionário para o ExcelWriter (Chave=Aba, Valor=DataFrame)
        return {
            "Rentabilidade_Armazem": resultado_final,
            "Consolidado_DRE": self.razao_para_download,
            "De_Paras_Não_Encontrados": df_nas_depara_razao,
            "De_Paras_Rateio_Não_Encontrados": df_nas_depara_rateio
        }