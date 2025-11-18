import streamlit as st
import pandas as pd
import os
import operator
import io
import pdfplumber
import json
import gc
import re

from qvd import qvd_reader
from datetime import datetime, date


#################### Relatórios Avançados ####################
operadores = {
    "+": operator.add,
    "-": operator.sub,
    "*": operator.mul,
    "/": operator.truediv,
    "**": operator.pow,
    "//": operator.floordiv,
    "%": operator.mod
}

operadores_group_by = {
    "Somar": "sum",
    "Contar": "count",
    "Contar Distinto": "nunique",
    "Média": "mean"
}

string_markdown=(                                    """
                                    **Formatos de dados:**
                                    - **`object`**: Texto  
                                    - **`float64`**: Numérico (Decimal - Exemplo: `2.50`, `3.14`)  
                                    - **`int64`**: Numérico (inteiro - Exemplo: `2`, `3`)  
                                    - **`bool`**: Booleano (`True`/`False`)  
                                    - **`datetime64[ns]`**: Data/Hora  
                                    - **`category`**: Categorias (valores únicos)  
                                    - **`timedelta[ns]`**: Diferença entre tempos
                                    """)

ajusa_separador = """Se sim, digite o separador." \
                    "Exemplo: 
                    caso queira concatenar coluna ano e peso, o nome da coluna seria anopeso, 
                    caso queira usar traço - para separar o nome das colunas, seria ano-peso"""

def gerar_arquivo_download(df):
    """
    Função para gerar o arquivo Excel baseado no DataFrame e permitir o download.
    """
    deseja_mudar_nome = st.selectbox(
        "Deseja mudar o nome do arquivo?",
        ["Selecione", "Sim", "Não"],
        key="deseja_mudar_nome"
    )

    nome_arquivo = "arquivo"
    if deseja_mudar_nome == "Sim":
        nome_arquivo = st.text_input("Digite o nome do arquivo (sem extensão):", key="nome_arquivo") or "arquivo"
    elif deseja_mudar_nome == "Não":
        nome_arquivo = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    else:
        st.write("Nenhum arquivo gerado.")
        return

    if nome_arquivo:
        buffer = io.BytesIO()
        df.to_excel(buffer, index=False, engine='openpyxl')
        buffer.seek(0)
        st.download_button(
            label="📥 Baixar Arquivo",
            data=buffer,
            file_name=f"{nome_arquivo}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    buffer.close()
    del buffer
    # Limpa flags se necessário
    if "download_config" in st.session_state:
        del st.session_state["download_config"]

class Historico:
    def __init__(self):
        if "historico_passos" not in st.session_state:
            st.session_state.historico_passos = []

    def limpar_historico(self):
        if "historico_passos" in st.session_state:
            st.session_state.historico_passos = []

    def limpar_json(self):
        if "json_historico" in st.session_state:
            del st.session_state["json_historico"]

    def inserir_historico(self, acao, params):
        st.session_state.historico_passos.append({
            "acao": acao,
            "params": params
        })

    def mostrar_historico(self):
        return st.session_state.historico_passos
    
    def exportar_historico(self):
        if "historico_passos" in st.session_state:
            st.session_state["json_historico"] = json.dumps(st.session_state.historico_passos.copy(), indent=2)
            
    def importar_historico(self):
        historico_json = st.file_uploader("📥 Importar Histórico (JSON)", type="json")
        if historico_json:
            try:
                historico = json.load(historico_json)
                st.session_state.historico_importado = historico
                st.success("✅ Histórico importado com sucesso!")
            except Exception as e:
                st.error(f"Erro ao importar JSON: {e}")

historico = Historico()

class Upload_Arquivos:
    def __init__(self,arquivo):

        if arquivo.name.endswith((".xlsx", ".xls")):
            self.update_arquivo_excel(arquivo)
        elif arquivo.name.endswith(".pdf"):
            self.update_arquivo_pdf(arquivo)
        elif arquivo.name.endswith(".parquet"):
            self.update_parquet(arquivo)
    
    def update_arquivo_excel(self, arquivo):
        if "files" not in st.session_state:
            st.session_state.files = pd.ExcelFile(arquivo)
            st.session_state.abas = st.session_state.files.sheet_names
            st.success("📊 Arquivo Excel carregado com sucesso!")
            if "nome_arquivo_carregado" not in st.session_state:
                st.session_state["nome_arquivo_carregado"] = arquivo.name

        abas = st.session_state.abas
        st.markdown("### Escolha a aba e a linha do cabeçalho")

        aba_input = st.selectbox("📑 Selecione a aba", options=abas)
        header_input = st.number_input("🔢 Número da linha do cabeçalho (0-index)",step =1,placeholder=None)

        if st.button("🔢 Clique ao Selecionar os Dados"):
            if aba_input is not None and header_input is not None:
                try:
                    df = pd.read_excel(
                        st.session_state.files,
                        sheet_name=aba_input,
                        header=int(header_input)
                    )
                    df.columns = df.columns.str.strip()
                    st.session_state.dados = df
                    st.session_state.df_fun = df.copy()
                    Historico().limpar_historico()
                    st.success("✅ Planilha Excel carregada com sucesso.")
                except Exception as e:
                    st.error(f"❌ Erro ao ler a aba: {e}")

    def update_parquet(self, arquivo):
        if "files" not in st.session_state:
            st.session_state.files = arquivo
            try:
                df = pd.read_parquet(
                    st.session_state.files)
                df.columns = df.columns.str.strip()
                st.session_state.dados = df
                st.session_state.df_fun = df.copy()
                Historico().limpar_historico()
                st.success("✅ Planilha Excel carregada com sucesso.")
            except Exception as e:
                st.error(f"❌ Erro ao ler a aba: {e}")
    
    def update_arquivo_pdf(self, arquivo):    
        st.markdown("### Escolha as páginas do PDF")

        if "files" not in st.session_state:
            st.session_state.files = arquivo

        with pdfplumber.open(st.session_state.files) as pdf:
            opcoes_paginas = [f"Página {i+1}" for i in range(len(pdf.pages))]

            paginas_selecionadas = st.multiselect(
                "📑 Selecione as páginas",
                options=opcoes_paginas,
                placeholder=None,
                key="numero_paginas"
            )

            if st.button("🔢 Clique ao Selecionar as Páginas") and paginas_selecionadas:
                try:
                    indices_paginas = [int(p.split()[1]) - 1 for p in paginas_selecionadas]
                    dfs_pdf = []

                    for i in indices_paginas:
                        pagina = pdf.pages[i]
                        tabela_raw = pagina.extract_table()

                        if tabela_raw:
                            df_tabela = pd.DataFrame(tabela_raw[1:], columns=tabela_raw[0])
                            dfs_pdf.append(df_tabela)

                    if dfs_pdf:
                        df = pd.concat(dfs_pdf, ignore_index=True)
                        df.columns = df.columns.str.strip()
                        if "nome_arquivo_carregado" not in st.session_state:
                            st.session_state["nome_arquivo_carregado"] = arquivo.name
                        st.session_state.dados = df
                        st.session_state.df_fun = df.copy()
                        Historico().limpar_historico()
                        st.success("✅ Tabelas do PDF extraídas com sucesso!")
                    else:
                        st.warning("⚠️ Nenhuma tabela válida encontrada nas páginas selecionadas.")
                except Exception as e:
                    st.error(f"❌ Erro ao processar o PDF: {e}")

class Funcoes_tabelas:
    def remover_colunas(self, df, coluna):
        df = df.drop(columns=coluna)
        Historico().inserir_historico("remover_colunas", {"coluna": coluna})      
        return df
    
    def renomear_colunas(self, df, novo_nomes):
        df = df.rename(columns=novo_nomes)
        Historico().inserir_historico("renomear_colunas", {"novo_nomes": novo_nomes})     
        return df

    def agrupamentos(self, df, coluna,agrupamentos):
        df = df.groupby(coluna, as_index=False).agg(**agrupamentos)
        Historico().inserir_historico("agrupamentos", {"coluna": coluna, "agrupamentos": agrupamentos})     
        return df

class Funcoes_colunas:
    def converter_int(self, df, coluna):
        df[coluna] = df[coluna].astype('int64')
        Historico().inserir_historico("converter_int", {"coluna": coluna})
        return df

    def converter_float(self, df, coluna):
        df[coluna] = df[coluna].astype('float64')
        Historico().inserir_historico("converter_float", {"coluna": coluna})
        return df

    def converter_string(self, df, coluna):
        df[coluna] = df[coluna].astype(str)
        Historico().inserir_historico("converter_string", {"coluna": coluna})
        return df

    def converter_datetime(self, df, colunas, formato="%Y-%m-%d"):
        for col in colunas:
            try:
                df[col] = pd.to_datetime(df[col], format=formato, errors="coerce")
            except Exception as e:
                raise ValueError(f"Erro ao converter a coluna '{col}' para datetime: {e}")
            
        Historico().inserir_historico("converter_datetime", {"coluna": colunas,"formato":formato})
        return df

    def extrair_mes(self, df, coluna):
        df[f'{coluna}_mes']=df[coluna].fillna(-1)
        df[f'{coluna}_mes']=df[f'{coluna}_mes'].dt.month
        df[f'{coluna}_mes']=df[f'{coluna}_mes'].astype('int64')
        Historico().inserir_historico("extrair_mes", {"coluna": coluna})
        return df

    def extrair_ano(self, df, coluna):
        df[f'{coluna}_ano']=df[coluna].fillna(-1)
        df[f'{coluna}_ano']=df[f'{coluna}_ano'].dt.year
        df[f'{coluna}_ano']=df[f'{coluna}_ano'].astype('int64')
        Historico().inserir_historico("extrair_ano", {"coluna": coluna})
        return df

    def preencher_vazios(self, df, coluna, valor):
        df[coluna] = df[coluna].fillna(valor)
        Historico().inserir_historico("preencher_vazios", {"coluna": coluna, "valor": valor})
        return df

    def remover_duplicados(self, df, coluna):
        nome_coluna = coluna[0]
        df = df.drop_duplicates(subset=nome_coluna)
        Historico().inserir_historico("remover_duplicados", {"coluna": coluna})
        return df

    def limpar_colunas(self, df, coluna):
        df[coluna] = df[coluna].replace(r'^\s*$', None, regex=True)
        df[coluna] = df[coluna].apply(lambda x: None if isinstance(x, str) and (x.strip() == '' or x.strip() == ' ') else x)
        Historico().inserir_historico("limpar_colunas", {"coluna": coluna})
        return df
    
    def concatenar_colunas(self, df, coluna, separador):
        if separador == "Não":
            separador = ""

        col_nome = separador.join(coluna)
        df[col_nome] = df[coluna].astype(str).agg(separador.join, axis=1)
        Historico().inserir_historico("concatenar_colunas", {"coluna": coluna, "separador": separador})
        return df

    def operacoes_matematicas(self, df, coluna, operacao, tipo_expressao = "Linhas"):
        if tipo_expressao == "Linhas":
            df[coluna] = df[coluna].apply(operacao)
            
        elif tipo_expressao == "Colunas":
            operador_str, coluna_alvo = operacao

            if operador_str not in operadores:
                raise ValueError(f"Operador '{operador_str}' inválido.")

            operador_func = operadores[operador_str]

            if isinstance(coluna, list):
                coluna = coluna[0]

            nova_coluna = f"{coluna}_{operador_str}_{coluna_alvo}"
            df[nova_coluna] = operador_func(df[coluna], df[coluna_alvo])
        else:
            raise ValueError("Tipo de expressão inválido")
        Historico().inserir_historico("operacoes_matematicas", {"coluna": coluna, "operacao": operacao, "tipo_expressao": tipo_expressao})
        return df

    def filtrar_coluna(self, df, condicao):
        """
        Filtra o DataFrame com base nas condições fornecidas
        
        Args:
            df: DataFrame pandas
            condicao: Dicionário no formato {coluna: (operador, valor)}
                    ou {coluna: (operador, (valor1, valor2))} para 'entre'
        
        Returns:
            DataFrame filtrado
        """
        OPERACOES = {

            # Numéricos
            ">": lambda col, val: col > val,
            "<": lambda col, val: col < val,
            "==": lambda col, val: col == val,
            ">=": lambda col, val: col >= val,
            "<=": lambda col, val: col <= val,
            "!=": lambda col, val: col != val,
            "Entre": lambda col, vals: col.between(vals[0], vals[1]),
            
            # Textuais
            "igual a": lambda col, val: col.astype(str).str.lower() == val.lower(),
            "contém": lambda col, val: col.astype(str).str.contains(val, case=False, na=False),
            "não contém": lambda col, val: ~col.astype(str).str.contains(val, case=False, na=False),
            "começa com": lambda col, val: col.astype(str).str.startswith(val, na=False),
            "termina com": lambda col, val: col.astype(str).str.endswith(val, na=False)
        }
        
        for coluna, (operador, valor) in condicao.items():
            try:
                if operador not in OPERACOES:
                    raise ValueError(f"Operador '{operador}' inválido")
                
                if coluna not in df.columns:
                    raise ValueError(f"Coluna '{coluna}' não encontrada")
                
                # Aplica o filtro
                mask = OPERACOES[operador](df[coluna], valor)
                df = df[mask]
                
            except Exception as e:
                st.error(f"❌ Erro ao filtrar coluna '{coluna}': {str(e)}")
                continue
        Historico().inserir_historico("filtrar_coluna", {"coluna": coluna, "condicao": condicao})        
        return df

lista_funcoes_colunas = [
                    "🔢 Converter para int", 
                    "🔢 Converter para float", 
                    "🔢 Converter para string",
                    "🔢 Converter para datetime",
                    "🔢 Extrair Mês",
                    "🔢 Extrair Ano",
                    "🔢 Limpar Colunas",
                    "🔢 Concatenar Colunas",
                    "🔢 Preencher Vazios",
                    "🔢 Remover Duplicados",
                    "🔢 Operações Matematicas",
                    "🔢 Filtrar Coluna",
                    ]

lista_funcoes_df = ["🔢 Renomear Colunas",
                    "🔢 Remover Colunas",
                    "🔢 Agrupamentos",
                    "🔢 Envio de Email"]

funcoes = Funcoes_colunas()

mapa_funcoes = {
    "🔢 Converter para int": {"func": funcoes.converter_int, "inputs": ["coluna"]},
    "🔢 Converter para float": {"func": funcoes.converter_float, "inputs": ["coluna"]},
    "🔢 Converter para string": {"func": funcoes.converter_string, "inputs": ["coluna"]},
    "🔢 Converter para datetime": {"func": funcoes.converter_datetime, "inputs": ["coluna","formato"]},
    "🔢 Extrair Mês": {"func": funcoes.extrair_mes, "inputs": ["coluna"]},
    "🔢 Extrair Ano": {"func": funcoes.extrair_ano, "inputs": ["coluna"]},
    "🔢 Concatenar Colunas": {"func": funcoes.concatenar_colunas, "inputs": ["coluna","Separador_Concatenar"]},    
    "🔢 Preencher Vazios": {"func": funcoes.preencher_vazios, "inputs": ["coluna", "valor"]},
    "🔢 Remover Duplicados": {"func": funcoes.remover_duplicados, "inputs": ["coluna"]},
    "🔢 Limpar Colunas": {"func": funcoes.limpar_colunas, "inputs": ["coluna"]},
    "🔢 Operações Matematicas": {"func": funcoes.operacoes_matematicas, "inputs": ["coluna", "operacao","tipo_expressao"]},
    "🔢 Filtrar Coluna": {"func": funcoes.filtrar_coluna, "inputs": ["coluna","condicao"]},
}

variavel_funcoes_tabela = Funcoes_tabelas()

mapa_funcoes_tabela = {
    "🔢 Remover Colunas": {"func": variavel_funcoes_tabela.remover_colunas, "inputs": ["coluna"]},
    "🔢 Renomear Colunas": {"func": variavel_funcoes_tabela.renomear_colunas, "inputs": ["coluna","novos_nomes_colunas"]},
    "🔢 Agrupamentos": {"func": variavel_funcoes_tabela.agrupamentos, "inputs": ["coluna","agrupamentos"]},
}

##############################

###############Relatórios Padrão ###############

class Json:

    caminho_json = "caminhos_dados.json"
    caminho_json_ctc = "caminhos_dados_ctc.json"
    caminho_json_ctc_colunas = "caminhos_dados_ctc_colunas.json"
    caminho_json_natureza_ecobox = "caminhos_ctcs_ecobox.xlsx"
    caminho_json_dimensoes_ecobox = "dimensoes_ecobox.xlsx"

    caminho_json_rentabilidade_armazem_arquivos = "caminhos_dados_rentabilidade_armazem_arquivos.json"
    caminho_json_rentabilidade_armazem_dados = "caminho_json_rentabilidade_armazem_dados.json"

    de_para_excel = "De_Para_Rentabilidade_Razao.xlsx"
    
    @staticmethod
    def gerar_json_padrao():
        json_padrao = {
            "dados_os_agregados": {
                "path": "\\\\172.16.200.206\\Luftlogistics\\CONTROLADORIA\\CUSTOS DE FRETES\\Consolidados_Agregados2025.xlsx",
                "sheet_name": "BD -  AGREGADOS 2025",
                "Colunas":["OS","Nro OS"]
            },
            "dados_os_frota": {
                "path": "\\\\172.16.200.206\\Luftlogistics\\CONTROLADORIA\\CUSTOS DE FRETES\\Frota - 2024_2025.xlsx",
                "sheet_name": "Frota",
                "Colunas":["OS","Nro OS","Nro Os"],
                "Coluna_Data":["Dt Emissão"],
                "Ano":[2025]
            },
            "dados_df_356": {
                "path": "C:\\Users\\lucas.brecht\\Desktop\\Projetos\\Rentabilidade_Frota\\Relatorio_356_1.xlsx",
                "sheet_name": "356",
                "Colunas_Os":["Nro OS"],
                "Coluna_CTC":["CTC"]
            }}
        with open(Json.caminho_json, "w",encoding="utf-8") as f:
            json.dump(json_padrao, f, indent=4)
        return json_padrao        

    @staticmethod
    def gerar_json_padrao_ctc():
        # Nenhuma alteração necessária nesta função, ela já está correta.
        json_padrao_ctc={
            "dados_atualizar_ctc": {
                "path": "\\\\172.16.200.206\\Luftlogistics\\PUBLICO\\LUCAS\\NFs_CTCs.qvd",
                "path_output": "\\\\172.16.200.206\\Luftlogistics\\PUBLICO\\LUCAS\\NFs_CTCs.parquet",
                "path_os": "\\\\172.16.200.206\\Luftlogistics\\PUBLICO\\LUCAS\\OrdServicos.qvd",
                "path_os_output": "\\\\172.16.200.206\\Luftlogistics\\PUBLICO\\LUCAS\\OrdServicos.parquet",
                "path_base": "\\\\172.16.200.206\\Luftlogistics\\PUBLICO\\LUCAS\\OrdServicosBase.qvd",
                "path_base_output": "\\\\172.16.200.206\\Luftlogistics\\PUBLICO\\LUCAS\\OrdServicosBase.parquet",
            },
            "dados_atualizar_parquet": {
                "path": "\\\\172.16.200.206\\Luftlogistics\\PUBLICO\\LUCAS\\NFs_CTCs.parquet",
                "path_os": "\\\\172.16.200.206\\Luftlogistics\\PUBLICO\\LUCAS\\NFs_CTCs.parquet",
                "path_os_base": "\\\\172.16.200.206\\Luftlogistics\\PUBLICO\\LUCAS\\NFs_CTCs.parquet",
            }
        }
        with open(Json.caminho_json_ctc, "w", encoding="utf-8") as f:
            json.dump(json_padrao_ctc, f, indent=4)
        return json_padrao_ctc

    @staticmethod
    def gerar_json_colunas_padrao_ctc():
        colunas_ctc={
            "colunas": {
                "colunas_fundamentais": ["Banco","Ano","Mês","CTC","Transportadora","Unid Resp Entrega","corresp","filialctc_origem",
                                        "Tipo Documento","DataCTC","NotaFiscal","Serie","Consignatario","Tabela",
                                        "CNPJ Remetente","CNPJ Dest","Data Agendmto","Prev Entrega Efetiva","Data Entrega Efetiva",
                                        "CNPJ Consig","Valor NF","Peso NF","Peso","Peso Cubado","Volumes NF","Valor Frete Bruto"],
                "colunas_adicionais": ["Quem Agenda","HRToTrGelo","Modal","Motivo","Espécie","Natureza","Remetente",
                                    "Destinatario","UF Origem","Cid Origem","UFEntrega","CidadeEntrega","Cidade Dest","Rota"],
                "colunas_farma": ["Banco","CTC","corresp","NotaFiscal","Valor Frete Bruto","Consignatario","CNPJ Consig"],
                "df_intec_limpeza_ctc": ["CTC", "filialctc_origem"],
                "df_farma_limpeza_ctc": ["CTC","corresp"],
                "df_remover_duplicados": ["CTC","NotaFiscal"],
                "mascara_cob": ["Peso", "Peso Cubado", "Peso NF", "Volumes NF"],
                "Peso_base": ["Peso", "Peso Cubado"]
            },
            "colunas_groupby": {
                "Valor Frete Bruto": "mean",
                "Peso": "mean",
                "Peso NF": "sum",
                "Volumes NF": "sum",
                "Valor NF": "sum",
                "Peso Cubado": "mean",
                "Peso Base": "mean"
            },
            "colunas_ctc_completa":{"Colunas": ["DtUltimaOcor","DataCTC","IdCTC","HoraCTC","Data_proc_CTC","Data_proc_NF","CTC",
            "corresp","Tipo Documento","ctc_comp","filialctc_origem","tem_ocorr","ImagemCTC","Nro Protocolo",
            "Motivo","Banco","NotaFiscal","Serie","Chave","DataNF","UltimaOcorNF","Desc_UltimaOcorNF","Filial Origem",
            "Remetente","CNPJ Remetente","Consignatario","CNPJ Consig","Destinatario","CNPJ Dest","Cidade Dest","UF Dest",
            "Grupo","Tipo Destinatario","UF Origem","Cid Origem","UF Remetente","Cid Remetente","UFEntrega","CidadeEntrega",
            "RegiaoEntrega", "RegioGeoEntrega","Unid Resp Emissão","Unid Resp Entrega","Transportadora","UltimaOcorrCTCCodOcorr",
            "Descr Ultima Ocor","Rota","Valor NF","Aliq. ICMS","Valor ICMS","Peso NF","Peso","Peso Cubado","Volumes NF","Tabela",
            "Valor Frete Bruto","Agendamentos Cod.91","HR_Ag_Efetivada","Recebedor Entrega","FreteLiquido","Rateio frete/NF",
            "Modal","Tipo Carga","Espécie","Natureza","TT Contratado","Status Ocorrencia","Status NF","PrevEntregaOriginal",
            "Prev Entrega Efetiva","Data Entrega Efetiva","Hora Entrega","Data Baixa Efetiva","Hora Baixa Efetiva","Horario Corte",
            "TT Real","Tem abono","Status_Abono_NF","Numero CTE","Serie CTE","Prioridade","Status CTC","Data Solic Agendmto","Data Agendmto",
            "Data Retorno Cliente","Cadastro no Agendador","Dt Cad Agendador","Agendmto Automatico (s/n)","Dt Cad Agdmto Automatico","Quem Agenda",
            "Romaneio","Codigo AWB","Cia AWB","Data AWB","Aerop Origem","Aerop Destino","Status AWB","Data Status AWB","Tipo Serviço AWB",
            "Tem Protocolo","Data Recepção","Status Imagem","Tem Imagem","Data Disponível","Nro Outro CTC","Origem","URL","Tem Canhoto",
            "Tem Imagem Canhoto","URL Canhoto","Nro Protocolo Canhoto","Data Recepção Canhoto","Data Disponivel Canhoto","idcodigo",
            "numpedido","CepEntrega","TabPrazo","Fatura","HRToTrGelo"]},
            "colunas_ctc_base":{
                "colunas_padrao":["Unid Resp Entrega","Transportadora","CTC","corresp","filialctc_origem",
                                    "Ano","Mês","DataCTC","Quem Agenda","Data Agendmto","Prev Entrega Efetiva","Data Entrega Efetiva",
                                    "HRToTrGelo","Nota Fiscal","Modal","Motivo","Espécie","Natureza","Remetente","CNPJ Remetente",
                                    "CNPJ Raiz - Remetente","Consignatario Intec","Consignatario","CNPJ Consig",
                                    "Destinatario","CNPJ Dest","CNPJ Raiz - Dest","UF Origem","Cid Origem","UFEntrega","CidadeEntrega",
                                    "Cidade Dest","Rota","Valor NF","Peso","Peso NF","Peso Cubado","Peso Base",
                                    "Volumes NF","Valor Frete Bruto Intec","Valor Frete Bruto Farma","Valor Frete Bruto"],
                "colu_cnpj":["CNPJ Remetente","CNPJ Dest"],
                "colu_data":["DataCTC","Prev Entrega Efetiva","Data Entrega Efetiva","Data Agendmto"],
                "formato_data":["%Y-%m-%d"], # <-- ALTERADO para o padrão ISO
                "colu_ano":[2024]},
        "ecobox": {
            "colunas_entrada": [
                "Ano","Mês","CTC","corresp","filialctc_origem","Tabela","Modal","DataCTC","Espécie",
                "Natureza","Motivo","UF Origem","UFEntrega","Consignatario",
                "Remetente","Destinatario","Valor NF","Peso","Peso Cubado",
                "Volumes NF","Valor Frete Bruto"
            ],
            "colunas_saida": [
                "Ano","Mês","DataCTC","CTC","corresp","filialctc_origem","Tabela",
                "Modal","Motivo","Espécie","Natureza","Tipo Ecobox",
                "UF Origem","UFEntrega","Consignatario","Remetente",
                "Destinatario","Valor NF","Peso","Volumes NF",
                "Valor Frete Bruto","Valor Uni. EcoBox","Liquidez"
            ],
            "arquivo_preços":{
                "columns":[
                    "Dimensão", # <-- CORRIGIDO o erro de digitação ("Dimenssão_")
                    "Inicio Vigência",
                    "Fim Vigência",
                    "Valor Uni. EcoBox"
                ],
                "data":[
                    # AS DATAS AGORA ESTÃO NO PADRÃO ISO (AAAA-MM-DD)
                    ["Eco05LT", "2025-01-01", "2025-12-31", 82.45],
                    ["Eco08LT", "2025-01-01", "2025-12-31", 104.65],
                    ["Eco12LT", "2023-02-28", "2023-08-31", 110.31],
                    ["Eco12LT", "2023-09-01", "2024-11-30", 115.4],
                    ["Eco12LT", "2024-12-01", "2025-12-31", 120.5],
                    ["Eco22LT", "2023-02-28", "2023-08-31", 153.86],
                    ["Eco22LT", "2023-09-01", "2024-11-30", 160.95],
                    ["Eco22LT", "2024-12-01", "2025-12-31", 168.06],
                    ["Eco35LT", "2023-02-28", "2023-08-31", 179.99],
                    ["Eco35LT", "2023-09-01", "2024-11-30", 188.28],
                    ["Eco35LT", "2024-12-01", "2025-12-31", 196.6],
                    ["Eco75LT", "2023-09-01", "2024-11-30", 437.91],
                    ["Eco75LT", "2024-12-01", "2025-12-31", 457.26]
                ]
            },
            "naturezas_ecobox":{
                "columns":[
                    "Natureza",
                    "Tipo Ecobox",
                    "Dimensão"
                ],
                "data":[
                    ["ASTELLAS - ECOBOX 5L", "ECOBOX 5L", "Eco05LT"],
                    ["ASTELLAS - ECOBOX 8L", "ECOBOX 8L", "Eco08LT"],
                    ["ASTELLAS - ECOBOX 12L", "ECOBOX 12L", "Eco12LT"],
                    ["ASTELLAS - ECOBOX 35L", "ECOBOX 35L", "Eco35LT"],
                    ["ASTELLAS - REV. ECOBOX 5L", "ECOBOX 5L", "Eco05LT"],
                    ["BAGO ECOBOX 5L", "ECOBOX 5L", "Eco05LT"],
                    ["LEO - ECOBOX 5L", "ECOBOX 5L", "Eco05LT"],
                    ["ECOBOX 5L - 48H", "ECOBOX 5L", "Eco05LT"],
                    ["ECOBOX 5L", "ECOBOX 5L", "Eco05LT"],
                    ["ECOBOX 8L", "ECOBOX 8L", "Eco08LT"],
                    ["BAGO ECOBOX 8L", "ECOBOX 8L", "Eco08LT"],
                    ["LEO - ECOBOX 8L", "ECOBOX 8L", "Eco08LT"],
                    ["ECOBOX 8L - 48H", "ECOBOX 8L", "Eco08LT"],
                    ["BAGO ECOBOX 12L", "ECOBOX 12L", "Eco12LT"],
                    ["LEO - ECOBOX 12L", "ECOBOX 12L", "Eco12LT"],
                    ["ECOBOX 12L", "ECOBOX 12L", "Eco12LT"],
                    ["ECOBOX 12L - 48H", "ECOBOX 12L", "Eco12LT"],
                    ["ECOBOX 22L", "ECOBOX 22L", "Eco22LT"],
                    ["LEO - ECOBOX 22L", "ECOBOX 22L", "Eco22LT"],
                    ["ECOBOX 22L - 48H", "ECOBOX 22L", "Eco22LT"],
                    ["BAGO ECOBOX 22L", "ECOBOX 22L", "Eco22LT"],
                    ["ECOBOX 35L", "ECOBOX 35L", "Eco35LT"],
                    ["BAGO ECOBOX 35L", "ECOBOX 35L", "Eco35LT"],
                    ["ECOBOX 35L - 48H", "ECOBOX 35L", "Eco35LT"],
                    ["LEO - ECOBOX 35L", "ECOBOX 35L", "Eco35LT"],
                    ["Gilead-Ecobox 75L-Rev 48H", "ECOBOX 75L", "Eco75LT"],
                    ["GILEAD-ECOBOX 75L-ENT 48H", "ECOBOX 75L", "Eco75LT"],
                    ["GILEAD-ECOBOX 75L-ENT 24H", "ECOBOX 75L", "Eco75LT"]
                ]
            }
        }
    }
        with open(Json.caminho_json_ctc_colunas, "w",encoding="utf-8") as f:
            json.dump(colunas_ctc, f, indent=4)
        return colunas_ctc

    @staticmethod
    def carregar_json_interativo_ctc() -> dict:
        if not os.path.exists(Json.caminho_json_ctc):
            st.warning("Arquivo JSON não encontrado. Criando com valores padrão...")
            return Json.gerar_json_padrao_ctc()

        with open(Json.caminho_json_ctc, "r",encoding="utf-8") as f:
            caminhos_ctc = json.load(f)

        caminhos_invalidos_ctc = {
            nome: info for nome, info in caminhos_ctc.items()
            if not os.path.exists(info.get("path", ""))
        }

        if caminhos_invalidos_ctc:
            st.error("Alguns caminhos de arquivos não foram encontrados:")

            novos_caminhos_ctc = {}

            for nome, info in caminhos_invalidos_ctc.items():
                st.text(f"{nome} (atual): {info['path']}")
                novo = st.text_input(f"Informe o novo path para '{nome}':", key=f"input_{nome}")
                
                if novo:
                    novos_caminhos_ctc[nome] = novo

            if st.button("Salvar novos caminhos"):
                # Atualiza TODOS os caminhos com os inputs preenchidos
                for nome, novo_path in novos_caminhos_ctc.items():
                    caminhos_ctc[nome]["path"] = novo_path

                with open(Json.caminho_json, "w") as f:
                    json.dump(caminhos_ctc, f, indent=4)

                st.success("JSON atualizado com sucesso. Recarregue a aplicação.")
                st.stop()
        return caminhos_ctc

    @staticmethod
    def carregar_json_interativo() -> dict:
        if not os.path.exists(Json.caminho_json):
            st.warning("Arquivo JSON não encontrado. Criando com valores padrão...")
            st.session_state["json_interativo"] = Json.gerar_json_padrao()
            return st.session_state["json_interativo"]

        with open(Json.caminho_json, "r", encoding="utf-8") as f:
            caminhos = json.load(f)

        caminhos_invalidos = {
            nome: info for nome, info in caminhos.items()
            if not os.path.exists(info.get("path", ""))
        }

        if caminhos_invalidos:
            st.error("Alguns caminhos de arquivos não foram encontrados:")

            novos_caminhos = {}

            for nome, info in caminhos_invalidos.items():
                st.text(f"{nome} (atual): {info['path']}")
                novo = st.text_input(f"Informe o novo path para '{nome}':", key=f"input_{nome}")

                if novo:
                    novos_caminhos[nome] = novo

            if st.button("Salvar novos caminhos"):
                for nome, novo_path in novos_caminhos.items():
                    caminhos[nome]["path"] = novo_path

                with open(Json.caminho_json, "w", encoding="utf-8") as f:
                    json.dump(caminhos, f, indent=4, ensure_ascii=False)

                st.success("JSON atualizado com sucesso. Recarregue a aplicação.")
                st.session_state["json_interativo"] = caminhos
                st.rerun()            

        st.session_state["json_interativo"] = caminhos
        return st.session_state["json_interativo"]

    @staticmethod
    def carregar_json_interativo_Colunas_CTC() -> dict:
        if not os.path.exists(Json.caminho_json_ctc_colunas):
            st.warning("Arquivo JSON não encontrado. Criando com valores padrão...")
            return Json.gerar_json_colunas_padrao_ctc()

        with open(Json.caminho_json_ctc_colunas, "r", encoding="utf-8") as f:
            caminhos_ctc_colunas = json.load(f)

        return caminhos_ctc_colunas

    @staticmethod
    def carregar_dados_por_nome(nome_tabela: str) -> pd.DataFrame:
        Json.carregar_json_interativo()
        caminhos = Json.carregar_json_interativo()

        if nome_tabela not in caminhos:
            st.error(f"Tabela '{nome_tabela}' não encontrada no JSON.")
            st.stop()

        path = caminhos[nome_tabela]["path"]
        sheet = caminhos[nome_tabela]["sheet_name"]

        if not os.path.exists(path):
            st.error(f"O caminho especificado para '{nome_tabela}' não existe: {path}")
            st.stop()

        try:
            excel = pd.ExcelFile(path)
            return pd.read_excel(excel, sheet_name=sheet)

        except ValueError as e:
            if "Worksheet" in str(e) or "not found" in str(e):
                st.warning(f"Aba '{sheet}' não encontrada no arquivo: {path}")
                excel = pd.ExcelFile(path)
                abas_disponiveis = excel.sheet_names
                nova_aba = st.selectbox(f"Selecione a nova aba para '{nome_tabela}':", abas_disponiveis, key=f"sheet_{nome_tabela}")

                if st.button("Atualizar aba e carregar novamente", key=f"botao_{nome_tabela}"):
                    caminhos[nome_tabela]["sheet_name"] = nova_aba
                    with open(Json.caminho_json, "w") as f:
                        json.dump(caminhos, f, indent=4)
                    st.success(f"Aba atualizada para '{nova_aba}'. Recarregando...")

                    # Recursivamente tenta de novo com a aba corrigida
                    st.rerun()

                st.stop()
            else:
                raise e

    @staticmethod
    def atualizar_json_colunas_os_agregados(novas_colunas):
        with open(Json.caminho_json, "r") as file:
            dados = json.load(file)

        dados["dados_os_agregados"]["Colunas"] = novas_colunas

        with open(Json.caminho_json, "w") as file:
            json.dump(dados, file, indent=4)

        st.write("JSON atualizado com sucesso.")

    @staticmethod
    def atualizar_json_colunas_os_frota(novas_colunas):
        with open(Json.caminho_json, "r") as file:
            dados = json.load(file)

        dados["dados_os_frota"]["Colunas"] = novas_colunas

        with open(Json.caminho_json, "w") as file:
            json.dump(dados, file, indent=4)

        st.write("JSON atualizado com sucesso.")

    @staticmethod
    def atualizar_json_colunas_os_356_Os(novas_colunas):
        with open(Json.caminho_json, "r") as file:
            dados = json.load(file)

        dados["dados_df_356"]["Colunas_Os"] = novas_colunas

        with open(Json.caminho_json, "w") as file:
            json.dump(dados, file, indent=4)

        st.write("JSON atualizado com sucesso.")

    @staticmethod
    def atualizar_json_colunas_os_356_CTC(novas_colunas):
        with open(Json.caminho_json, "r") as file:
            dados = json.load(file)

        dados["dados_df_356"]["Coluna_CTC"] = novas_colunas

        with open(Json.caminho_json, "w") as file:
            json.dump(dados, file, indent=4)

        st.write("JSON atualizado com sucesso.")

    @staticmethod
    def atualizar_json_colunas_os_356_Frete(novas_colunas):
        with open(Json.caminho_json, "r") as file:
            dados = json.load(file)

        dados["dados_df_356"]["Colunas_Frete"] = novas_colunas

        with open(Json.caminho_json, "w") as file:
            json.dump(dados, file, indent=4)

        st.write("JSON atualizado com sucesso.")

    @staticmethod
    def atualizar_json_colunas_os_frota_data_emissao(novas_colunas):
        with open(Json.caminho_json, "r") as file:
            dados = json.load(file)

        dados["dados_os_frota"]["Coluna_Data"] = novas_colunas

        with open(Json.caminho_json, "w") as file:
            json.dump(dados, file, indent=4)

        st.write("JSON atualizado com sucesso.")

    @staticmethod
    def json_ano_data():
        st.write("##### Verificando a existências dos Jsons...")
        if os.path.exists(Json.caminho_json_ctc):
            with open(Json.caminho_json_ctc, "r") as f:
                path_parquet = json.load(f)
        else:
            st.warning("Arquivo JSON nao encontrado. Criando com valores padrao...")
            Json.gerar_json_padrao_ctc()
            with open(Json.caminho_json_ctc, "r") as f:
                path_parquet = json.load(f)

        st.write("##### Carregando Dados")
        caminho_parquet = path_parquet["dados_atualizar_parquet"]["path"]
        df = pd.read_parquet(caminho_parquet)

        st.write("##### Verificando Coluna DataCTC")
        if "DataCTC" not in df.columns:
            st.warning("Coluna não encontrada, selecione uma coluna válida para atualizar")
            coluna_escolhida =st.selectbox("Colunas", df.columns)
            if st.button("🔄 Atualizar"):
                df_data = pd.DataFrame(df[coluna_escolhida].copy())
                df_data = df_data.rename(columns={coluna_escolhida: "DataCTC"})
        else:   
            df_data = pd.DataFrame(df["DataCTC"].copy())
            st.dataframe(df_data)

        del df
        gc.collect()

        st.write("##### Convertendo Coluna DataCTC para formato de Data")
        df_data["DataCTC"] = pd.to_datetime(df_data["DataCTC"], errors="coerce", dayfirst=True)

        st.write("##### Extraindo Ano e Mes")
        df_data["Ano"] = df_data["DataCTC"].dt.year
        df_data["Mes"] = df_data["DataCTC"].dt.month

        st.write("##### Agrupando Ano e Mes")
        df_data = df_data.drop(columns=["DataCTC"]).dropna(subset=["Ano", "Mes"])
        df_data = (
            df_data.groupby("Ano")["Mes"]
            .unique()
            .apply(lambda x: sorted([int(i) for i in x]))
            .to_dict()
        )
        
        df_data = {int(ano): meses for ano, meses in df_data.items()}

        st.write("##### Salvando Json")
        if os.path.exists(Json.caminho_json_ctc_colunas):
            with open(Json.caminho_json_ctc_colunas, "r",encoding="utf-8") as f:
                colunas_datas = json.load(f)

            colunas_datas["colunas_ano_mes"] = df_data

            with open(Json.caminho_json_ctc_colunas, "w", encoding="utf-8") as f:
                json.dump(colunas_datas, f, indent=4, ensure_ascii=False)

        else:
            st.warning("Arquivo JSON nao encontrado. Criando com valores padrao...")
            Json.gerar_json_colunas_padrao_ctc()

            with open(Json.caminho_json_ctc_colunas, "r",encoding="utf-8") as f:
                colunas_datas = json.load(f)

            colunas_datas["colunas_ano_mes"] = df_data

            with open(Json.caminho_json_ctc_colunas, "w", encoding="utf-8") as f:
                json.dump(colunas_datas, f, indent=4, ensure_ascii=False)

        st.write("##### Finalizado")

class Json_Rentabilidade:
    @staticmethod
    def gerar_json_padrao_rentabilidade():
        if not os.path.exists(Json.caminho_json_rentabilidade_armazem_arquivos):
            json_padrao_rentabilidade={
                "Volumes_Base": {
                    "path": r"C:\\Projetos\\DRE\\Relatórios\\DREs\\Volumes - Base.xlsx",
                    "columns": ["SITE","CLIENTE","DATAFIMPEDIDO","CATEGORIAGRUPO","VOLUMES"]
                },
                "Faturamento": {
                    "path": "C:\\Projetos\\DRE\\Relatórios\\DREs\\Faturamento 2025.xlsx",
                    "columns": ["EMPRESA","FILIAL","CLIENTE","RECEITA","VERSÃO","MÊS","ANO","TIPO","VALOR R$"],
                    "sheet_name": "base",
                    "header":[6],
                    "empresa": ["FARMA","FARMA DIST"],
                    "ano":[2025],
                    "versao":["Real"],
                    "receita":["Serviços"]
                },
                "Insumos": {
                    "path": "C:\\Projetos\\DRE\\Relatórios\\DREs\\Insumos.xlsx",
                    "columns": ["ID","Mês","Depositante","NOMECLI","Custo","Insumo"]
                },
                "Adequacao": {
                    "path": "C:\\Projetos\\DRE\\Relatórios\\DREs\\Quantidade - Adequação.xlsx",
                    "columns": ["Filial","Cliente","Qtde Real","Nome Servico","Serviço","Data Fim"]
                },
                "DRE": {
                    "path": "C:\\Projetos\\DRE\\Relatórios\\DREs\\Resultado DRE Mensal 2025_v2.xlsx",
                    "sheet_name":["RAZÃO_FARMA","RAZÃO_FARMADIST"],
                    "header":[3],
                    "colunas_dre":["Conta","Título Conta","Data","Descrição","Filial","Centro de Custo","Item","saldo"],
                    "colunas_str": ["Conta","Item","Filial","Centro de Custo"]
                },
                "Ocupacao_Armazem": {
                    "path":"C:\\Projetos\\DRE\\Relatórios\\DREs\\Acompanhamento Pallets 2025.xlsx",
                    "sheet_name":["SP","SC","RJ","GO"],
                    "header":[4,5],
                    "columns": [
                        "M\u00eas",
                        "Cliente",
                        "Ocupa\u00e7\u00e3o",
                        "Filial",
                        "Ano"
                    ],
                    "escrita_mes":"Mês"
                },
                "De_Para": {
                    "path": "C:\\Projetos\\DRE\\Relatórios\\DREs\\De_Para_Rentabilidade_Razao.xlsx"
                    },                    
            }
            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w", encoding="utf-8") as f:
                json.dump(json_padrao_rentabilidade, f, indent=4)
            return json_padrao_rentabilidade
        else:
            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w", encoding="utf-8") as f:
                json.dump(json_padrao_rentabilidade, f, indent=4)
            return json_padrao_rentabilidade            

    @staticmethod
    def gerar_json_padrao_rentabilidade_de_para():
        if not os.path.exists(Json.caminho_json_rentabilidade_armazem_dados):
            json_padrao_rentabilidade_de_para={
            "De_Para": {
                "sheet_name":{
                "DRE_De_Para_Item_Conta": {
                "index": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    24,
                    25,
                    26,
                    27,
                    28,
                    29,
                    30,
                    31,
                    32,
                    33,
                    34,
                    35,
                    36,
                    37,
                    38,
                    39,
                    40,
                    41,
                    42,
                    43,
                    44,
                    45,
                    46,
                    47,
                    48,
                    49,
                    50,
                    51,
                    52,
                    53,
                    54,
                    55,
                    56,
                    57,
                    58,
                    59,
                    60,
                    61,
                    62,
                    63,
                    64,
                    65,
                    66,
                    67,
                    68,
                    69,
                    70,
                    71,
                    72,
                    73,
                    74,
                    75,
                    76,
                    77,
                    78,
                    79,
                    80,
                    81,
                    82,
                    83,
                    84,
                    85,
                    86,
                    87,
                    88,
                    89,
                    90,
                    91,
                    92,
                    93,
                    94,
                    95,
                    96,
                    97,
                    98,
                    99,
                    100,
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107,
                    108,
                    109,
                    110,
                    111,
                    112,
                    113
                ],
                "columns": [
                    "Item",
                    "Nome",
                    "Sigla"
                ],
                "data": [
                    [
                        10321,
                        "BAUSCH LOMB INDUSTRIA OTICA LTDA",
                        "BAUSCH"
                    ],
                    [
                        10399056,
                        "FINZELBERG ATIVOS NATURAIS LTDA",
                        "FINZELBERG"
                    ],
                    [
                        10399058,
                        "MOMENTA FARMACEUTICA LTDA",
                        "MOMENTA"
                    ],
                    [
                        "MLK",
                        "MOLKEM BRASIL LTDA",
                        "MOLKEM"
                    ],
                    [
                        "NVAA",
                        "NVAA COM. DE PRODUTOS DE PERFUMARIA E HIGIENE PESSOAL LTDA",
                        "SEMINA"
                    ],
                    [
                        "PBN",
                        "PBN QUIMICA E FARMACEUTICA LTDA",
                        "PBN - QUÌMICA"
                    ],
                    [
                        "STC",
                        "DISTRIBUIDORA DE MEDICAMENTOS SANTA CRUZ LTDA",
                        "SANTA CRUZ"
                    ],
                    [
                        10802,
                        "ADMINISTRATIVO / OPERACAO",
                        "Desconhecido"
                    ],
                    [
                        10803,
                        "SUN FARMA",
                        "SUN PHARMA"
                    ],
                    [
                        10806,
                        "RANBAXY FARMACEUTICA LTDA - GO",
                        "RANBAXY"
                    ],
                    [
                        11105,
                        "HEMOBRAS - JABOATÃO",
                        "HEMOBRAS"
                    ],
                    [
                        10702,
                        "ADMINISTRATIVO / OPERACAO",
                        "Desconhecido"
                    ],
                    [
                        10706,
                        "LAB DAUDT OLIVEIRA",
                        "MEGALABS"
                    ],
                    [
                        10708,
                        "CAPSUGEL",
                        "CAPSUGEL"
                    ],
                    [
                        10714,
                        "LUNDBECK BRASIL LTDA.",
                        "LUNDBECK"
                    ],
                    [
                        10715,
                        "RANBAXY FARMACEUTICA LTDA.",
                        "RANBAXY"
                    ],
                    [
                        10719,
                        "PROJETO ADEQUAÇÕES FILIAL RIO DE JANEIRO",
                        "Desconhecido"
                    ],
                    [
                        10720,
                        "PROJETO CF RAMBAXY",
                        "RANBAXY"
                    ],
                    [
                        10179,
                        "CÂMARA FRIA LONZA",
                        "Desconhecido"
                    ],
                    [
                        10181,
                        "TAKEDA AR CONDICIONADO RUA 5",
                        "TAKEDA"
                    ],
                    [
                        10187,
                        "PROJETO CÂMARA CONGELADA",
                        "Desconhecido"
                    ],
                    [
                        10197,
                        "PROJETO CÂMARA FRIA",
                        "Desconhecido"
                    ],
                    [
                        11002,
                        "ADMINISTRATIVO / OPERACAO",
                        "Desconhecido"
                    ],
                    [
                        11004,
                        "TAKEDA",
                        "TAKEDA"
                    ],
                    [
                        11005,
                        "AIRELA INDUSTRIA FARMACEUTICA LTDA",
                        "Desconhecido"
                    ],
                    [
                        11006,
                        "ANTIBIOTICOS DO BRASIL LTDA",
                        "ABL"
                    ],
                    [
                        11007,
                        "MEAD JOHNSON DO BRASIL COMERCIO E IMPORT",
                        "MEAD JOHNSON"
                    ],
                    [
                        11010,
                        "BEAUFOUR / IPSEN",
                        "IPSEN"
                    ],
                    [
                        11012,
                        "ISDIN PRODUTOS FARMACEUTICOS LTDA.",
                        "ISDIN"
                    ],
                    [
                        11013,
                        "MEIZLER UCB BIOPHARMA S.A.",
                        "UCB"
                    ],
                    [
                        11014,
                        "GRUNENTHAL DO BRASIL FARMACEUTICA LTDA.",
                        "GRUNENTHAL"
                    ],
                    [
                        11020,
                        "CAPSUGEL BRASIL IMPORT E DIST DE INSUMOS",
                        "CAPSUGEL"
                    ],
                    [
                        11021,
                        "MULTILAB IND E COM DE PROD FAR",
                        "MULTILAB"
                    ],
                    [
                        11022,
                        "GALDERMA",
                        "GALDERMA"
                    ],
                    [
                        11023,
                        "LABORATORIOS EXPANSCIENCE",
                        "EXPANSCIENCE"
                    ],
                    [
                        11025,
                        "NESTLE SKIN HEALTH BRASIL LTDA",
                        "GALDERMA"
                    ],
                    [
                        11026,
                        "UNICHEM FARMACEUTICA DO BRASIL LTDA - SC",
                        "UNICHEM"
                    ],
                    [
                        11028,
                        "PROJETO SEGREGACAO AREA 344 - MOKSHA8",
                        "MOKSHA8"
                    ],
                    [
                        11030,
                        "BLANVER",
                        "BLANVER"
                    ],
                    [
                        11031,
                        "MERZ",
                        "MERZ"
                    ],
                    [
                        11032,
                        "GRUNENTHAL SANTA CATARINA",
                        "GRUNENTHAL"
                    ],
                    [
                        11034,
                        "GILEAD SCIENCES FARMACEUTICA DO BRASIL LTDA",
                        "GILEAD"
                    ],
                    [
                        11039,
                        "MELHORIA DO LAYOUT DE EXPEDICAO ITAJAI  ",
                        "Desconhecido"
                    ],
                    [
                        11042,
                        "MOKSHA8 BRASIL INDÚSTRIA E COMÉRCIO DE MEDICAMENTOS LTDA/SC",
                        "MOKSHA8"
                    ],
                    [
                        "TH",
                        "THERAMEX FARMACEUTICA LTDA. /SC",
                        "THERAMEX"
                    ],
                    [
                        10166,
                        "Segregação de área Isdin",
                        "ISDIN"
                    ],
                    [
                        10101,
                        "DIRETORIA",
                        "Desconhecido"
                    ],
                    [
                        10102,
                        "ADMINISTRATIVO / OPERACAO",
                        "Desconhecido"
                    ],
                    [
                        10106,
                        "GASTOS INTERNACIONAIS",
                        "Desconhecido"
                    ],
                    [
                        10107,
                        "PROJETO 54",
                        "Desconhecido"
                    ],
                    [
                        10108,
                        "ADM ARMAZENAGEM",
                        "Desconhecido"
                    ],
                    [
                        10109,
                        "ADM TRANSPORTE",
                        "Desconhecido"
                    ],
                    [
                        10110,
                        "ADEQUACAO",
                        "ADEQUAÇÃO"
                    ],
                    [
                        10153,
                        "MANUTENCAO REFRIGERACAO",
                        "Desconhecido"
                    ],
                    [
                        10155,
                        "CONDOMINIO ELETRICA",
                        "Desconhecido"
                    ],
                    [
                        10190,
                        "não operacional",
                        "Desconhecido"
                    ],
                    [
                        10191,
                        "AMPLIACAO TAKEDA SAO PAULO",
                        "TAKEDA"
                    ],
                    [
                        10195,
                        "COVID 19",
                        "Desconhecido"
                    ],
                    [
                        10196,
                        "PROJETO CÂMARA FRIA G1",
                        "Desconhecido"
                    ],
                    [
                        10198,
                        "TAKEDA PROMOCIONAL",
                        "TAKEDA"
                    ],
                    [
                        10203,
                        "ADMINISTRATIVO / OPERACAO",
                        "Desconhecido"
                    ],
                    [
                        10302,
                        "ADMINISTRATIVO / OPERACAO",
                        "Desconhecido"
                    ],
                    [
                        10316,
                        "UNICHEM FARMACEUTICA DO BRASIL LTDA - SP",
                        "UNICHEM"
                    ],
                    [
                        10323,
                        "TAKEDA",
                        "TAKEDA"
                    ],
                    [
                        10327,
                        "GALDERMA",
                        "GALDERMA"
                    ],
                    [
                        10345,
                        "LABORATORIO FERRING",
                        "Desconhecido"
                    ],
                    [
                        10356,
                        "SHIRE HUMAN",
                        "SHIRE"
                    ],
                    [
                        10357,
                        "BEAUFOUR / IPSEN",
                        "IPSEN"
                    ],
                    [
                        10364,
                        "ASTELLAS",
                        "ASTELLAS"
                    ],
                    [
                        10371,
                        "ISDIN PRODUTOS FARMACEUTICOS LTDA.",
                        "ISDIN"
                    ],
                    [
                        10386,
                        "LEO PHARMA LTDA",
                        "LEO PHARMA"
                    ],
                    [
                        10389,
                        "MERCK SHARP & DOHME FARMACEUTICA LTDA",
                        "MERCK"
                    ],
                    [
                        10718,
                        "SUN FARMACEUTICA DO BRASIL LTDA - RJ",
                        "SUN PHARMA"
                    ],
                    [
                        11036,
                        "MBB - MAWDSLEYS PHARMACEUTICALS DO BRASIL LTDA",
                        "MAWDSLEYS"
                    ],
                    [
                        11202,
                        "ADMINISTRATIVO / OPERACAO",
                        "Desconhecido"
                    ],
                    [
                        11502,
                        "ADMINISTRATIVO / OPERACAO",
                        "Desconhecido"
                    ],
                    [
                        11604,
                        "HEMOBRAS - EMPRESA BRASILEIRA DE HEMODER",
                        "HEMOBRAS"
                    ],
                    [
                        1100303,
                        "THERAMEX",
                        "THERAMEX"
                    ],
                    [
                        10399003,
                        "MOKSHA8 BRASIL DISTRIBUIDORA E REPRESENT",
                        "MOKSHA8"
                    ],
                    [
                        10399006,
                        "SEMINA INDUSTRIA E COMERCIO LTDA.",
                        "SEMINA"
                    ],
                    [
                        10399007,
                        "RECKITT BENCKISER BRASIL LTDA.",
                        "RECKITT"
                    ],
                    [
                        10399011,
                        "GRUNENTHAL DO BRASIL FARMACEUTICA LTDA.",
                        "GRUNENTHAL"
                    ],
                    [
                        10399012,
                        "LABORATORIOS BALDACCI LTDA",
                        "BALDACCI"
                    ],
                    [
                        10399013,
                        "ABBOTT LABORATORIOS DO BRASIL LTDA",
                        "ABBOTT"
                    ],
                    [
                        10399016,
                        "LABORATORIO EXPANSCIENCE COMERCIO IMPORT",
                        "EXPANSCIENCE"
                    ],
                    [
                        10399017,
                        "SUNSTAR BRASIL IMPORTADORA E DIST. LTDA.",
                        "SUNSTAR"
                    ],
                    [
                        10399020,
                        "RANBAXY FARMACEUTICA LTDA.",
                        "RANBAXY"
                    ],
                    [
                        10399024,
                        "LABORATORIOS BIODERMA",
                        "NAOS BRASIL"
                    ],
                    [
                        10399030,
                        "CELLERA",
                        "CELLERA"
                    ],
                    [
                        10399037,
                        "BLANVER",
                        "BLANVER"
                    ],
                    [
                        10399045,
                        "CYG BIOTECH QUIMICA & FARMACEUTICA LTDA",
                        "CYG BIOTECH"
                    ],
                    [
                        10399047,
                        "EXELTIS",
                        "EXELTIS"
                    ],
                    [
                        10399050,
                        "EUROFARMA",
                        "EUROFARMA"
                    ],
                    [
                        10399054,
                        "JCR ITAPEVI",
                        "JCR"
                    ],
                    [
                        10399055,
                        "PROJETO REDUCAO AREA TAKEDA",
                        "TAKEDA"
                    ],
                    [
                        10399059,
                        "ASTRAZENECA DO BRASIL LTDA.",
                        "ASTRAZENECA"
                    ],
                    [
                        1030409,
                        "ASTRAZENECA DO BRASIL LTDA",
                        "ASTRAZENECA"
                    ],
                    [
                        10192,
                        "PROJETO 304",
                        "Desconhecido"
                    ],
                    [
                        10399061,
                        "PROJETO CENTRAL MONITORAMENTO TEMPERATURA",
                        "Desconhecido"
                    ],
                    [
                        11033,
                        "TAKEDA DISTRIBUIDORA",
                        "TAKEDA"
                    ],
                    [
                        10399021,
                        "LUNDBECK BRASIL LTDA.",
                        "LUNDBECK"
                    ],
                    [
                        11041,
                        "SEMINA INDUSTRIA E COMERCIO LTDA",
                        "SEMINA"
                    ],
                    [
                        10399064,
                        "Roquette Brasil",
                        "ROQUETTE"
                    ],
                    [
                        10315,
                        "OMNICARE",
                        "Desconhecido"
                    ],
                    [
                        10185,
                        "PROJETO RB - Embu das Artes",
                        "Desconhecido"
                    ],
                    [
                        11203,
                        "OPERACOES",
                        "Desconhecido"
                    ],
                    [
                        10152,
                        "MANUTENCAO ELETRICA",
                        "Desconhecido"
                    ],
                    [
                        10363,
                        "BRISTOL",
                        "BRISTOL"
                    ],
                    [
                        10154,
                        "CONDOMINIO PREDIAL",
                        "Desconhecido"
                    ],
                    [
                        10172,
                        "Adequação do Sistema de Refrigeração RJ",
                        "Desconhecido"
                    ],
                    [
                        11008,
                        "GENOMMA LABORATORIES DO BRASIL LTDA",
                        "GENOMMA"
                    ],
                    [
                        101,
                        "Farma Distribuidora",
                        "Desconhecido"
                    ],
                    [
                        102,
                        "Farma Distribuidora",
                        "Desconhecido"
                    ],
                    [
                        10396,
                        "MIP BRASIL INDUSTRIA E COMERCIO",
                        "CELLERA"
                    ]
                ]
            },
            "DRE_De_Para_Centro_Custo": {
                "index": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    24,
                    25,
                    26,
                    27,
                    28,
                    29,
                    30,
                    31,
                    32,
                    33,
                    34,
                    35,
                    36,
                    37,
                    38,
                    39,
                    40,
                    41
                ],
                "columns": [
                    "Centro de Custo",
                    "Centro Custo",
                    "Tipo CC"
                ],
                "data": [
                    [
                        21122001,
                        "Operação Armazenagem",
                        "Oper"
                    ],
                    [
                        21110101,
                        "Motoristas coleta / distribuição",
                        "Oper"
                    ],
                    [
                        21110102,
                        "Motoristas viagens",
                        "Oper"
                    ],
                    [
                        21110401,
                        "Operação Transporte",
                        "Oper"
                    ],
                    [
                        21110403,
                        "Torre de Controle",
                        "Oper"
                    ],
                    [
                        21110404,
                        "Torre de Controle",
                        "Oper"
                    ],
                    [
                        21110406,
                        "Operação Armazenagem",
                        "Oper"
                    ],
                    [
                        21110501,
                        "Gestão de Frota",
                        "Oper"
                    ],
                    [
                        21120101,
                        "SAC",
                        "Oper"
                    ],
                    [
                        21110201,
                        "Manutenção de frota",
                        "Oper"
                    ],
                    [
                        21110301,
                        "Segurança Patrimonial",
                        "Oper"
                    ],
                    [
                        21110402,
                        "Gerenciamento de Riscos",
                        "Oper"
                    ],
                    [
                        21120202,
                        "ESG-S",
                        "Oper"
                    ],
                    [
                        21120203,
                        "Qualidade",
                        "Oper"
                    ],
                    [
                        21120301,
                        "Facilities",
                        "Oper"
                    ],
                    [
                        21120106,
                        "ESG-S",
                        "Oper"
                    ],
                    [
                        25110101,
                        "Presidência",
                        "Adm"
                    ],
                    [
                        25110202,
                        "Corporativo",
                        "Adm"
                    ],
                    [
                        25110302,
                        "Projetos",
                        "Adm"
                    ],
                    [
                        25110401,
                        "Tesouraria",
                        "Adm"
                    ],
                    [
                        25110404,
                        "Gestão de Fretes",
                        "Adm"
                    ],
                    [
                        25110503,
                        "Controladoria",
                        "Adm"
                    ],
                    [
                        25110601,
                        "Recursos Humanos",
                        "Adm"
                    ],
                    [
                        25110901,
                        "Suprimentos",
                        "Adm"
                    ],
                    [
                        25111001,
                        "Tecnologia da Informação",
                        "Adm"
                    ],
                    [
                        25111101,
                        "Jurídico",
                        "Adm"
                    ],
                    [
                        25110201,
                        "Marketing",
                        "Coml"
                    ],
                    [
                        25110301,
                        "Comercial",
                        "Coml"
                    ],
                    [
                        25110801,
                        "SAC",
                        "Coml"
                    ],
                    [
                        21110405,
                        "Recebimento",
                        "Oper"
                    ],
                    [
                        21111001,
                        "Operação Transporte",
                        "Oper"
                    ],
                    [
                        21120104,
                        "Expedição",
                        "Oper"
                    ],
                    [
                        21120401,
                        "Gestão de Operação",
                        "Oper"
                    ],
                    [
                        25110701,
                        "Qualidade",
                        "Adm"
                    ],
                    [
                        25110702,
                        "HSE",
                        "Oper"
                    ],
                    [
                        25110703,
                        "HSE",
                        "Oper"
                    ],
                    [
                        25112001,
                        "Despesas Administrativas",
                        "Adm"
                    ],
                    [
                        25110802,
                        "SAC",
                        "Adm"
                    ],
                    [
                        21110203,
                        "Facilities",
                        "Oper"
                    ],
                    [
                        25110402,
                        "Tesouraria",
                        "Adm"
                    ],
                    [
                        25110501,
                        "Controladoria",
                        "Adm"
                    ],
                    [
                        25110405,
                        "Diretoria",
                        "Adm"
                    ]
                ]
            },
            "DRE_De_Para_Filial": {
                "index": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23
                ],
                "columns": [
                    "Filial",
                    "Filial UF"
                ],
                "data": [
                    [
                        "ITAPEVI",
                        "SP"
                    ],
                    [
                        "GOIÂNIA",
                        "GO"
                    ],
                    [
                        "ITAJAI",
                        "SC"
                    ],
                    [
                        "MATRIZ",
                        "SP"
                    ],
                    [
                        "RIO DE JANEIRO",
                        "RJ"
                    ],
                    [
                        "ITAJAÍ",
                        "SC"
                    ],
                    [
                        "GO",
                        "GO"
                    ],
                    [
                        "VIX",
                        "SP"
                    ],
                    [
                        "JANDIRA",
                        "SP"
                    ],
                    [
                        "ITAPEVI 15",
                        "SP"
                    ],
                    [
                        "BARUERI",
                        "SP"
                    ],
                    [
                        "ABREU E LIMA",
                        "SP"
                    ],
                    [
                        "JABOATÃO",
                        "SP"
                    ],
                    [
                        "PROJETOS",
                        "SP"
                    ],
                    [
                        "Segregação de área Isdin",
                        "SP"
                    ],
                    [
                        5,
                        "SP"
                    ],
                    [
                        6,
                        "RJ"
                    ],
                    [
                        10,
                        "SC"
                    ],
                    [
                        1,
                        "SP"
                    ],
                    [
                        2,
                        "SC"
                    ],
                    [
                        3,
                        "GO"
                    ],
                    [
                        4,
                        "SP"
                    ],
                    [
                        15,
                        "SC"
                    ],
                    [
                        17,
                        "SP"
                    ]
                ]
            },
            "DRE_De_Para_Contas_Contabeis": {
                "index": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    24,
                    25,
                    26,
                    27,
                    28,
                    29,
                    30,
                    31,
                    32,
                    33,
                    34,
                    35,
                    36,
                    37,
                    38,
                    39,
                    40,
                    41,
                    42,
                    43,
                    44,
                    45,
                    46,
                    47,
                    48,
                    49,
                    50,
                    51,
                    52,
                    53,
                    54,
                    55,
                    56,
                    57,
                    58,
                    59,
                    60,
                    61,
                    62,
                    63,
                    64,
                    65,
                    66,
                    67,
                    68,
                    69,
                    70,
                    71,
                    72,
                    73,
                    74,
                    75,
                    76,
                    77,
                    78,
                    79,
                    80,
                    81,
                    82,
                    83,
                    84,
                    85,
                    86,
                    87,
                    88,
                    89,
                    90,
                    91,
                    92,
                    93,
                    94,
                    95,
                    96,
                    97,
                    98,
                    99,
                    100,
                    101,
                    102,
                    103,
                    104,
                    105,
                    106,
                    107,
                    108,
                    109,
                    110,
                    111,
                    112,
                    113,
                    114,
                    115,
                    116,
                    117,
                    118,
                    119,
                    120,
                    121,
                    122,
                    123,
                    124,
                    125,
                    126,
                    127,
                    128,
                    129,
                    130,
                    131,
                    132,
                    133,
                    134,
                    135,
                    136,
                    137,
                    138,
                    139,
                    140,
                    141,
                    142,
                    143,
                    144,
                    145,
                    146,
                    147,
                    148,
                    149,
                    150,
                    151,
                    152,
                    153,
                    154,
                    155,
                    156,
                    157,
                    158,
                    159,
                    160,
                    161,
                    162,
                    163,
                    164,
                    165,
                    166,
                    167,
                    168,
                    169,
                    170,
                    171,
                    172,
                    173,
                    174,
                    175,
                    176,
                    177,
                    178,
                    179,
                    180,
                    181,
                    182,
                    183,
                    184,
                    185,
                    186,
                    187,
                    188,
                    189,
                    190,
                    191,
                    192,
                    193,
                    194,
                    195,
                    196,
                    197,
                    198,
                    199,
                    200,
                    201,
                    202,
                    203,
                    204,
                    205,
                    206,
                    207,
                    208,
                    209,
                    210,
                    211,
                    212,
                    213,
                    214,
                    215,
                    216,
                    217,
                    218,
                    219,
                    220,
                    221,
                    222,
                    223,
                    224,
                    225,
                    226,
                    227,
                    228,
                    229,
                    230,
                    231,
                    232,
                    233,
                    234,
                    235,
                    236,
                    237,
                    238,
                    239,
                    240,
                    241,
                    242,
                    243,
                    244,
                    245,
                    246,
                    247,
                    248,
                    249,
                    250,
                    251,
                    252,
                    253,
                    254,
                    255,
                    256,
                    257,
                    258,
                    259,
                    260,
                    261,
                    262,
                    263,
                    264,
                    265,
                    266,
                    267,
                    268,
                    269,
                    270,
                    271,
                    272,
                    273,
                    274,
                    275,
                    276,
                    277,
                    278,
                    279,
                    280,
                    281,
                    282,
                    283,
                    284,
                    285,
                    286,
                    287,
                    288,
                    289,
                    290,
                    291,
                    292,
                    293,
                    294,
                    295,
                    296,
                    297,
                    298,
                    299,
                    300,
                    301,
                    302,
                    303,
                    304,
                    305,
                    306,
                    307,
                    308,
                    309,
                    310,
                    311,
                    312,
                    313,
                    314,
                    315,
                    316,
                    317,
                    318,
                    319,
                    320,
                    321,
                    322,
                    323,
                    324,
                    325,
                    326,
                    327,
                    328,
                    329,
                    330,
                    331,
                    332,
                    333,
                    334,
                    335,
                    336,
                    337,
                    338,
                    339,
                    340,
                    341,
                    342,
                    343,
                    344,
                    345,
                    346,
                    347,
                    348,
                    349,
                    350,
                    351,
                    352,
                    353,
                    354,
                    355,
                    356,
                    357,
                    358,
                    359,
                    360,
                    361,
                    362,
                    363,
                    364,
                    365,
                    366,
                    367,
                    368,
                    369,
                    370,
                    371,
                    372,
                    373,
                    374,
                    375,
                    376,
                    377,
                    378,
                    379,
                    380,
                    381,
                    382,
                    383,
                    384,
                    385,
                    386,
                    387,
                    388,
                    389,
                    390,
                    391,
                    392,
                    393,
                    394,
                    395,
                    396,
                    397,
                    398,
                    399,
                    400,
                    401,
                    402,
                    403,
                    404,
                    405,
                    406,
                    407,
                    408,
                    409,
                    410,
                    411,
                    412,
                    413,
                    414,
                    415,
                    416,
                    417
                ],
                "columns": [
                    "Concat Razão",
                    "conta",
                    "descrição completa",
                    "descrição resumida",
                    "Grupo",
                    "Grupo Financeiro"
                ],
                "data": [
                    [
                        "NaN",
                        60101010201,
                        "FRETES",
                        "FRETES",
                        "FRETES",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60101010203,
                        "SERVIÇOS",
                        "SERVIÇOS",
                        "SERVIÇOS",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        "60101010203A",
                        "SERVIÇOS - POLO SC",
                        "SERVIÇOS POLO SC",
                        "SERVIÇOS POLO SC",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60101020204,
                        "ISS",
                        "ISS",
                        "ISS",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60101020201,
                        "COFINS",
                        "COFINS",
                        "COFINS",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60101020202,
                        "PIS",
                        "PIS",
                        "PIS",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60101020203,
                        "ICMS",
                        "ICMS",
                        "ICMS",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60301020265,
                        "CRÉDITO ICMS PRESUMIDO SP",
                        "CRED. PRESUM. ICMS",
                        "CRED. PRESUM. ICMS",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60301050106,
                        "(-) Descontos",
                        "DESCONTOS",
                        "DESCONTOS",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        "60301020283A",
                        "(-) Terceiros - Polo SC",
                        "TERCEIROS POLO SC",
                        "TERCEIROS POLO SC",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60301020232,
                        "(-) Mat. Embalagem",
                        "MAT EMBA",
                        "MAT EMBA",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60301020226,
                        "INDEN.MERCADORIAS",
                        "INDEN.MERCADORIAS",
                        "INDEN.MERCADORIAS",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        "60301020226A",
                        "(-) Indenizações de Mercadorias",
                        "INDEN.MERCADORIAS",
                        "INDEN.MERCADORIAS",
                        "Não Financeiro"
                    ],
                    [
                        "60301020101Oper",
                        60301020101,
                        "SALARIOS",
                        "SALARIOS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020103Oper",
                        60301020103,
                        "SALARIOS",
                        "SALARIOS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020129Oper",
                        60301020129,
                        "SALARIOS",
                        "SALARIOS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020130Oper",
                        60301020130,
                        "SALARIOS",
                        "SALARIOS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020131Oper",
                        60301020131,
                        "SALARIOS",
                        "SALARIOS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020132Oper",
                        60301020132,
                        "SALARIOS",
                        "SALARIOS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020134Oper",
                        60301020134,
                        "SALARIOS",
                        "SALARIOS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020102Oper",
                        60301020102,
                        "HORAS EXTRAS",
                        "HE OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020105Oper",
                        60301020105,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020107Oper",
                        60301020107,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020120Oper",
                        60301020120,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020121Oper",
                        60301020121,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020124Oper",
                        60301020124,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020125Oper",
                        60301020125,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020126Oper",
                        60301020126,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020127Oper",
                        60301020127,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020110Oper",
                        60301020110,
                        "INSS FOPAG",
                        "INSS FOPAG OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020112Oper",
                        60301020112,
                        "INSS FOPAG",
                        "INSS FOPAG OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020111Oper",
                        60301020111,
                        "FGTS FOPAG",
                        "FGTS FOPAG OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020123Oper",
                        60301020123,
                        "INSS EMPRESA-TERCEIR",
                        "INSS EMPRESA OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020128Oper",
                        60301020128,
                        "INSS EMPRESA-TERCEIR",
                        "INSS EMPRESA OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020109Oper",
                        60301020109,
                        "ASSIST.MEDICA/ODONTO",
                        "ASSIST MED OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020108Oper",
                        60301020108,
                        "INDEN. TRAB.",
                        "INDEN. TRAB. OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020108AOper",
                        "60301020108A",
                        "INDEN. TRAB.",
                        "INDEN. TRAB. OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020104Oper",
                        60301020104,
                        "INDEN. TRAB.",
                        "AVISO PREVIO OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020117Oper",
                        60301020117,
                        "ESTAGIOS",
                        "ESTAGIOS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020119Oper",
                        60301020119,
                        "TREINAMENTOS",
                        "TREINAMENTOS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020122Oper",
                        60301020122,
                        "PRO-LABORE",
                        "PRO-LABORE OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020133Oper",
                        60301020133,
                        "MEDICINA SEG.NO TRAB",
                        "MED TRAB OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020115Oper",
                        60301020115,
                        "BENEF.VALE TRANSPORT",
                        "VALE TRANSP OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020114Oper",
                        60301020114,
                        "CEST BASICA/ALIM TRA",
                        "VALE ALIM OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020116Oper",
                        60301020116,
                        "CEST BASICA/ALIM TRA",
                        "VALE ALIM OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020135Oper",
                        60301020135,
                        "OUTROS GASTOS C/PESS",
                        "OUTROS PESS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020113Oper",
                        60301020113,
                        "OUTROS GASTOS C/PESS",
                        "OUTROS PESS OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020279Oper",
                        60301020279,
                        "ASSESS. SERV. OPERAC",
                        "ASSESS. OPERAC OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020209Oper",
                        60301020209,
                        "CARGA/DESCARGA",
                        "CARGA/DESCARGA OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020219Oper",
                        60301020219,
                        "EQUIP. PROTEÇÃO",
                        "EQUIP. PROTEÇÃO OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020258Oper",
                        60301020258,
                        "UNIFORMES",
                        "UNIFORMES OPER",
                        "PESSOAL OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020101Adm",
                        60301020101,
                        "SALARIOS",
                        "SALARIOS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020103Adm",
                        60301020103,
                        "SALARIOS",
                        "SALARIOS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020129Adm",
                        60301020129,
                        "SALARIOS",
                        "SALARIOS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020130Adm",
                        60301020130,
                        "SALARIOS",
                        "SALARIOS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020131Adm",
                        60301020131,
                        "SALARIOS",
                        "SALARIOS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020132Adm",
                        60301020132,
                        "SALARIOS",
                        "SALARIOS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020134Adm",
                        60301020134,
                        "SALARIOS",
                        "SALARIOS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020102Adm",
                        60301020102,
                        "HORAS EXTRAS",
                        "HE ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020105Adm",
                        60301020105,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020107Adm",
                        60301020107,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020120Adm",
                        60301020120,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020121Adm",
                        60301020121,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020124Adm",
                        60301020124,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020125Adm",
                        60301020125,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020126Adm",
                        60301020126,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020127Adm",
                        60301020127,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020110Adm",
                        60301020110,
                        "INSS FOPAG",
                        "INSS FOPAG ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020112Adm",
                        60301020112,
                        "INSS FOPAG",
                        "INSS FOPAG ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020111Adm",
                        60301020111,
                        "FGTS FOPAG",
                        "FGTS FOPAG ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020123Adm",
                        60301020123,
                        "INSS EMPRESA-TERCEIR",
                        "INSS EMPRESA ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020128Adm",
                        60301020128,
                        "INSS EMPRESA-TERCEIR",
                        "INSS EMPRESA ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020109Adm",
                        60301020109,
                        "ASSIST.MEDICA/ODONTO",
                        "ASSIST MED ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020108Adm",
                        60301020108,
                        "INDEN. TRAB.",
                        "INDEN. TRAB. ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020108AAdm",
                        "60301020108A",
                        "INDEN. TRAB.",
                        "INDEN. TRAB. ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020104Adm",
                        60301020104,
                        "INDEN. TRAB.",
                        "AVISO PREVIO ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020117Adm",
                        60301020117,
                        "ESTAGIOS",
                        "ESTAGIOS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020119Adm",
                        60301020119,
                        "TREINAMENTOS",
                        "TREINAMENTOS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020122Adm",
                        60301020122,
                        "PRO-LABORE",
                        "PRO-LABORE ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020133Adm",
                        60301020133,
                        "MEDICINA SEG.NO TRAB",
                        "MED TRAB ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020115Adm",
                        60301020115,
                        "BENEF.VALE TRANSPORT",
                        "VALE TRANSP ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020114Adm",
                        60301020114,
                        "CEST BASICA/ALIM TRA",
                        "VALE ALIM ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020116Adm",
                        60301020116,
                        "CEST BASICA/ALIM TRA",
                        "VALE ALIM ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020135Adm",
                        60301020135,
                        "OUTROS GASTOS C/PESS",
                        "OUTROS PESS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020113Adm",
                        60301020113,
                        "OUTROS GASTOS C/PESS",
                        "OUTROS PESS ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020279Adm",
                        60301020279,
                        "ASSESS. SERV. OPERAC",
                        "ASSESS. OPERAC ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020209Adm",
                        60301020209,
                        "CARGA/DESCARGA",
                        "CARGA/DESCARGA ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020248Adm",
                        60301020248,
                        "CARGA/DESCARGA",
                        "CARGA/DESCARGA ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020219Adm",
                        60301020219,
                        "EQUIP. PROTEÇÃO",
                        "EQUIP. PROTEÇÃO ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020258Adm",
                        60301020258,
                        "UNIFORMES",
                        "UNIFORMES ADMN",
                        "PESSOAL ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020101Coml",
                        60301020101,
                        "SALARIOS",
                        "SALARIOS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020103Coml",
                        60301020103,
                        "SALARIOS",
                        "SALARIOS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020129Coml",
                        60301020129,
                        "SALARIOS",
                        "SALARIOS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020130Coml",
                        60301020130,
                        "SALARIOS",
                        "SALARIOS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020131Coml",
                        60301020131,
                        "SALARIOS",
                        "SALARIOS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020132Coml",
                        60301020132,
                        "SALARIOS",
                        "SALARIOS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020134Coml",
                        60301020134,
                        "SALARIOS",
                        "SALARIOS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020102Coml",
                        60301020102,
                        "HORAS EXTRAS",
                        "HE COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020105Coml",
                        60301020105,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020107Coml",
                        60301020107,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020120Coml",
                        60301020120,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020121Coml",
                        60301020121,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020124Coml",
                        60301020124,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020125Coml",
                        60301020125,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020126Coml",
                        60301020126,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020127Coml",
                        60301020127,
                        "FERIAS E 13 SALARIO",
                        "FÉRIAS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020110Coml",
                        60301020110,
                        "INSS FOPAG",
                        "INSS FOPAG COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020112Coml",
                        60301020112,
                        "INSS FOPAG",
                        "INSS FOPAG COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020111Coml",
                        60301020111,
                        "FGTS FOPAG",
                        "FGTS FOPAG COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020123Coml",
                        60301020123,
                        "INSS EMPRESA-TERCEIR",
                        "INSS EMPRESA COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020128Coml",
                        60301020128,
                        "INSS EMPRESA-TERCEIR",
                        "INSS EMPRESA COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020109Coml",
                        60301020109,
                        "ASSIST.MEDICA/ODONTO",
                        "ASSIST MED COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020108Coml",
                        60301020108,
                        "INDEN. TRAB.",
                        "INDEN. TRAB. COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020108AComl",
                        "60301020108A",
                        "INDEN. TRAB.",
                        "INDEN. TRAB. COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020104Coml",
                        60301020104,
                        "INDEN. TRAB.",
                        "AVISO PREVIO COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020117Coml",
                        60301020117,
                        "ESTAGIOS",
                        "ESTAGIOS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020119Coml",
                        60301020119,
                        "TREINAMENTOS",
                        "TREINAMENTOS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020122Coml",
                        60301020122,
                        "PRO-LABORE",
                        "PRO-LABORE COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020133Coml",
                        60301020133,
                        "MEDICINA SEG.NO TRAB",
                        "MED TRAB COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020115Coml",
                        60301020115,
                        "BENEF.VALE TRANSPORT",
                        "VALE TRANSP COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020114Coml",
                        60301020114,
                        "CEST BASICA/ALIM TRA",
                        "VALE ALIM COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020116Coml",
                        60301020116,
                        "CEST BASICA/ALIM TRA",
                        "VALE ALIM COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020135Coml",
                        60301020135,
                        "OUTROS GASTOS C/PESS",
                        "OUTROS PESS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020113Coml",
                        60301020113,
                        "OUTROS GASTOS C/PESS",
                        "OUTROS PESS COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020279Coml",
                        60301020279,
                        "ASSESS. SERV. OPERAC",
                        "ASSESS. OPERAC COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020209Coml",
                        60301020209,
                        "CARGA/DESCARGA",
                        "CARGA/DESCARGA COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020248Coml",
                        60301020248,
                        "CARGA/DESCARGA",
                        "CARGA/DESCARGA COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020219Coml",
                        60301020219,
                        "EQUIP. PROTEÇÃO",
                        "EQUIP. PROTEÇÃO COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020258Coml",
                        60301020258,
                        "UNIFORMES",
                        "UNIFORMES COML",
                        "PESSOAL COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020290Oper",
                        60301020290,
                        "FRETES SUBCONTRATADOS",
                        "FRETES SUBCONTRATADOS OPER",
                        "FRETES SUBCONTRATADOS OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020221Oper",
                        60301020221,
                        "FRETES SUBCONTRATADOS",
                        "FRETES PJ OPER",
                        "FRETES PJ OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020291Oper",
                        60301020291,
                        "FRETES SUBCONTRATADOS",
                        "FRETES SUBCONTRATADOS OPER",
                        "FRETES SUBCONTRATADOS OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020290AOper",
                        "60301020290A",
                        "FRETES INTEC - RODOVIÁRIO",
                        "FRETES INTEC - RODOVIÁRIO OPER",
                        "FRETES INTEC - RODOVIÁRIO OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020288Oper",
                        60301020288,
                        "FRETES INTEC - AÉREO",
                        "FRETES INTEC - AÉREO OPER",
                        "FRETES INTEC - AÉREO OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020221AOper",
                        "60301020221A",
                        "FRETES CONSÓRCIO",
                        "FRETES CONSÓRCIO OPER",
                        "FRETES CONSÓRCIO OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020290Adm",
                        60301020290,
                        "FRETES SUBCONTRATADOS",
                        "FRETES SUBCONTRATADOS ADMN",
                        "FRETES SUBCONTRATADOS ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020221Adm",
                        60301020221,
                        "FRETES SUBCONTRATADOS",
                        "FRETES PJ ADMN",
                        "FRETES PJ ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020291Adm",
                        60301020291,
                        "FRETES SUBCONTRATADOS",
                        "FRETES SUBCONTRATADOS ADMN",
                        "FRETES SUBCONTRATADOS ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020290AAdm",
                        "60301020290A",
                        "FRETES INTEC - RODOVIÁRIO",
                        "FRETES INTEC - RODOVIÁRIO ADMN",
                        "FRETES INTEC - RODOVIÁRIO ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020288Adm",
                        60301020288,
                        "FRETES INTEC - AÉREO",
                        "FRETES INTEC - AÉREO ADMN",
                        "FRETES INTEC - AÉREO ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020221AAdm",
                        "60301020221A",
                        "FRETES CONSÓRCIO",
                        "FRETES CONSÓRCIO ADMN",
                        "FRETES CONSÓRCIO ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020290Coml",
                        60301020290,
                        "FRETES SUBCONTRATADOS",
                        "FRETES SUBCONTRATADOS COML",
                        "FRETES SUBCONTRATADOS COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020221Coml",
                        60301020221,
                        "FRETES SUBCONTRATADOS",
                        "FRETES PJ COML",
                        "FRETES PJ COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020291Coml",
                        60301020291,
                        "FRETES SUBCONTRATADOS",
                        "FRETES SUBCONTRATADOS COML",
                        "FRETES SUBCONTRATADOS COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020290AComl",
                        "60301020290A",
                        "FRETES INTEC - RODOVIÁRIO",
                        "FRETES INTEC - RODOVIÁRIO COML",
                        "FRETES INTEC - RODOVIÁRIO COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020288Coml",
                        60301020288,
                        "FRETES INTEC - AÉREO",
                        "FRETES INTEC - AÉREO COML",
                        "FRETES INTEC - AÉREO COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020221AComl",
                        "60301020221A",
                        "FRETES CONSÓRCIO",
                        "FRETES CONSÓRCIO COML",
                        "FRETES CONSÓRCIO COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020210Oper",
                        60301020210,
                        "COMBUSTIVEIS",
                        "COMBUSTIVEIS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020229Oper",
                        60301020229,
                        "LUBRIFICANTES",
                        "LUBRIFICANTES OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020255Oper",
                        60301020255,
                        "VIAGENS/TRANSP - 255P",
                        "VIAGENS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020238Oper",
                        60301020238,
                        "PEDÁGIOS",
                        "PEDÁGIOS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020269Oper",
                        60301020269,
                        "ESTADIA - TRANSP -269F",
                        "ESTADIAS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020271Oper",
                        60301020271,
                        "TRANS PREMIOS - 271",
                        "PREMIOS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020270Oper",
                        60301020270,
                        "DESCARGAS",
                        "DESCARGAS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020243Oper",
                        60301020243,
                        "REP.CONSERV. VEICULOS",
                        "REP.CONSERV. VEIC OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020236Oper",
                        60301020236,
                        "PEÇAS P/ VEIC. EQUIP",
                        "PEÇAS P/ VEIC. EQUIP OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020212Oper",
                        60301020212,
                        "CONS. PNEUS/CAMARAS ",
                        "CONS. PNEUS/CAMARAS  OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020227Oper",
                        60301020227,
                        "IPVA",
                        "IPVA OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020241Oper",
                        60301020241,
                        "RECAPAGEM",
                        "RECAPAGEM OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020237Oper",
                        60301020237,
                        "PEÇAS E ACESSORIOS",
                        "PEÇAS E ACESSORIOS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020216Oper",
                        60301020216,
                        "CONSERV. TACOGRAFOS",
                        "CONSERV. TACOGRAFOS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020204Oper",
                        60301020204,
                        "ALUGUEL DE VEICULOS",
                        "ALUGUEL VEIC OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020239Oper",
                        60301020239,
                        "PNEUS E CAMARAS",
                        "PNEUS E CAMARAS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020292Oper",
                        60301020292,
                        "LAVAGEM DE VEICULOS",
                        "LAVAGEM DE VEICULOS OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301040105Oper",
                        60301040105,
                        "MULTAS INFR. TRANSITO",
                        "MULTAS TRANSITO OPER",
                        "FROTA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020210Adm",
                        60301020210,
                        "COMBUSTIVEIS",
                        "COMBUSTIVEIS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020229Adm",
                        60301020229,
                        "LUBRIFICANTES",
                        "LUBRIFICANTES ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020255Adm",
                        60301020255,
                        "VIAGENS/TRANSP - 255P",
                        "VIAGENS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020238Adm",
                        60301020238,
                        "PEDÁGIOS",
                        "PEDÁGIOS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020269Adm",
                        60301020269,
                        "ESTADIA - TRANSP -269F",
                        "ESTADIAS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020271Adm",
                        60301020271,
                        "TRANS PREMIOS - 271",
                        "PREMIOS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020270Adm",
                        60301020270,
                        "DESCARGAS",
                        "DESCARGAS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020243Adm",
                        60301020243,
                        "REP.CONSERV. VEICULOS",
                        "REP.CONSERV. VEIC ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020236Adm",
                        60301020236,
                        "PEÇAS P/ VEIC. EQUIP",
                        "PEÇAS P/ VEIC. EQUIP ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020212Adm",
                        60301020212,
                        "CONS. PNEUS/CAMARAS ",
                        "CONS. PNEUS/CAMARAS  ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020227Adm",
                        60301020227,
                        "IPVA",
                        "IPVA ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020241Adm",
                        60301020241,
                        "RECAPAGEM",
                        "RECAPAGEM ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020237Adm",
                        60301020237,
                        "PEÇAS E ACESSORIOS",
                        "PEÇAS E ACESSORIOS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020216Adm",
                        60301020216,
                        "CONSERV. TACOGRAFOS",
                        "CONSERV. TACOGRAFOS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020204Adm",
                        60301020204,
                        "ALUGUEL DE VEICULOS",
                        "ALUGUEL VEIC ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020239Adm",
                        60301020239,
                        "PNEUS E CAMARAS",
                        "PNEUS E CAMARAS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020292Adm",
                        60301020292,
                        "LAVAGEM DE VEICULOS",
                        "LAVAGEM DE VEICULOS ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301040105Adm",
                        60301040105,
                        "MULTAS INFR. TRANSITO",
                        "MULTAS TRANSITO ADMN",
                        "FROTA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020210Coml",
                        60301020210,
                        "COMBUSTIVEIS",
                        "COMBUSTIVEIS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020229Coml",
                        60301020229,
                        "LUBRIFICANTES",
                        "LUBRIFICANTES coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020255Coml",
                        60301020255,
                        "VIAGENS/TRANSP - 255P",
                        "VIAGENS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020238Coml",
                        60301020238,
                        "PEDÁGIOS",
                        "PEDÁGIOS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020269Coml",
                        60301020269,
                        "ESTADIA - TRANSP -269F",
                        "ESTADIAS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020271Coml",
                        60301020271,
                        "TRANS PREMIOS - 271",
                        "PREMIOS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020270Coml",
                        60301020270,
                        "DESCARGAS",
                        "DESCARGAS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020243Coml",
                        60301020243,
                        "REP.CONSERV. VEICULOS",
                        "REP.CONSERV. VEIC coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020236Coml",
                        60301020236,
                        "PEÇAS P/ VEIC. EQUIP",
                        "PEÇAS P/ VEIC. EQUIP coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020212Coml",
                        60301020212,
                        "CONS. PNEUS/CAMARAS ",
                        "CONS. PNEUS/CAMARAS  coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020227Coml",
                        60301020227,
                        "IPVA",
                        "IPVA coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020241Coml",
                        60301020241,
                        "RECAPAGEM",
                        "RECAPAGEM coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020237Coml",
                        60301020237,
                        "PEÇAS E ACESSORIOS",
                        "PEÇAS E ACESSORIOS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020216Coml",
                        60301020216,
                        "CONSERV. TACOGRAFOS",
                        "CONSERV. TACOGRAFOS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020204Coml",
                        60301020204,
                        "ALUGUEL DE VEICULOS",
                        "ALUGUEL VEIC coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020239Coml",
                        60301020239,
                        "PNEUS E CAMARAS",
                        "PNEUS E CAMARAS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020292Coml",
                        60301020292,
                        "LAVAGEM DE VEICULOS",
                        "LAVAGEM DE VEICULOS coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301040105Coml",
                        60301040105,
                        "MULTAS INFR. TRANSITO",
                        "MULTAS TRANSITO coml",
                        "FROTA coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020248Oper",
                        60301020248,
                        "SEGURO CARGAS RCTR-C",
                        "SEGURO CARGAS RCTR-C oper",
                        "GESTAO RISCO oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020249Oper",
                        60301020249,
                        "SEGURO CARGA RCF-DC",
                        "SEGURO CARGA RCF-DC oper",
                        "GESTAO RISCO oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020294Oper",
                        60301020294,
                        "SEGUROS RCTA",
                        "SEGUROS RCTA oper",
                        "GESTAO RISCO oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020264Oper",
                        60301020264,
                        "SERV. MONIT. AUTOTRAC",
                        "SERV. MONIT. AUTOTRAC oper",
                        "GESTAO RISCO oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020244Oper",
                        60301020244,
                        "SEG.ESCOLTA",
                        "SEG.ESCOLTA oper",
                        "GESTAO RISCO oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020246Oper",
                        60301020246,
                        "SEGURO - PREMIOS",
                        "SEGURO OBRIGATORIO oper",
                        "GESTAO RISCO oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020247Oper",
                        60301020247,
                        "SEGURO - PREMIOS",
                        "SEGURO - PREMIOS oper",
                        "GESTAO RISCO oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020262Oper",
                        60301020262,
                        "HONORÁRIOS/RISCO",
                        "HONORÁRIOS/RISCO oper",
                        "GESTAO RISCO oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020305Oper",
                        60301020305,
                        "HONORÁRIOS/RISCO",
                        "HONORÁRIOS/RISCO oper",
                        "GESTAO RISCO oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020249Adm",
                        60301020249,
                        "SEGURO CARGA RCF-DC",
                        "SEGURO CARGA RCF-DC admn",
                        "GESTAO RISCO admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020294Adm",
                        60301020294,
                        "SEGUROS RCTA",
                        "SEGUROS RCTA admn",
                        "GESTAO RISCO admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020264Adm",
                        60301020264,
                        "SERV. MONIT. AUTOTRAC",
                        "SERV. MONIT. AUTOTRAC admn",
                        "GESTAO RISCO admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020244Adm",
                        60301020244,
                        "SEG.ESCOLTA",
                        "SEG.ESCOLTA admn",
                        "GESTAO RISCO admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020246Adm",
                        60301020246,
                        "SEGURO - PREMIOS",
                        "SEGURO OBRIGATORIO admn",
                        "GESTAO RISCO admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020247Adm",
                        60301020247,
                        "SEGURO - PREMIOS",
                        "SEGURO - PREMIOS admn",
                        "GESTAO RISCO admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020262Adm",
                        60301020262,
                        "HONORÁRIOS/RISCO",
                        "HONORÁRIOS/RISCO admn",
                        "GESTAO RISCO admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020305Adm",
                        60301020305,
                        "HONORÁRIOS/RISCO",
                        "HONORÁRIOS/RISCO admn",
                        "GESTAO RISCO admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020249Coml",
                        60301020249,
                        "SEGURO CARGA RCF-DC",
                        "SEGURO CARGA RCF-DC coml",
                        "GESTAO RISCO coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020294Coml",
                        60301020294,
                        "SEGUROS RCTA",
                        "SEGUROS RCTA coml",
                        "GESTAO RISCO coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020264Coml",
                        60301020264,
                        "SERV. MONIT. AUTOTRAC",
                        "SERV. MONIT. AUTOTRAC coml",
                        "GESTAO RISCO coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020244Coml",
                        60301020244,
                        "SEG.ESCOLTA",
                        "SEG.ESCOLTA coml",
                        "GESTAO RISCO coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020246Coml",
                        60301020246,
                        "SEGURO - PREMIOS",
                        "SEGURO OBRIGATORIO coml",
                        "GESTAO RISCO coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020247Coml",
                        60301020247,
                        "SEGURO - PREMIOS",
                        "SEGURO - PREMIOS coml",
                        "GESTAO RISCO coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020262Coml",
                        60301020262,
                        "HONORÁRIOS/RISCO",
                        "HONORÁRIOS/RISCO coml",
                        "GESTAO RISCO coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020305Coml",
                        60301020305,
                        "HONORÁRIOS/RISCO",
                        "HONORÁRIOS/RISCO coml",
                        "GESTAO RISCO coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020203Oper",
                        60301020203,
                        "ALUG. IMÓVEIS",
                        "ALUG. IMÓVEIS OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020213Oper",
                        60301020213,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020214Oper",
                        60301020214,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020215Oper",
                        60301020215,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020287Oper",
                        60301020287,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020304Oper",
                        60301020304,
                        "NaN",
                        "CONSERV. PRÉDIOS OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020230Oper",
                        60301020230,
                        "ENERGIA ELETRICA",
                        "ENERGIA ELETRICA OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020210AOper",
                        "60301020210A",
                        "COMBUSTIVEL GERADOR",
                        "COMB GERADOR OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020272Oper",
                        60301020272,
                        "COMBUSTIVEL GERADOR",
                        "COMB GERADOR OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020256Oper",
                        60301020256,
                        "LIMPEZA",
                        "LIMPEZA OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020202Oper",
                        60301020202,
                        "AGUA ",
                        "AGUA OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020201Oper",
                        60301020201,
                        "ALUGUEL DE EQUIPAMENTOS",
                        "ALUGUEL EQUIP OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020250Oper",
                        60301020250,
                        "SEGURO PREDIOS",
                        "SEGURO PREDIOS OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020245Oper",
                        60301020245,
                        "SEGURANCA PREDIAL",
                        "SEGURANCA PREDIAL OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301040104Oper",
                        60301040104,
                        "IPTU",
                        "IPTU OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020252Oper",
                        60301020252,
                        "TELEFONES",
                        "TELEFONES OPER",
                        "INFORMATICA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020251Oper",
                        60301020251,
                        "PROVEDOR INTERNET",
                        "INTERNET OPER",
                        "INFORMATICA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020233Oper",
                        60301020233,
                        "MAT. HIGIENE/LIMPEZA ",
                        "MAT. HIGIENE/LIMPEZA OPER",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020203Adm",
                        60301020203,
                        "ALUG. IMÓVEIS",
                        "ALUG. IMÓVEIS ADMN",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020213Adm",
                        60301020213,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020214Adm",
                        60301020214,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020215Adm",
                        60301020215,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020287Adm",
                        60301020287,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020304Adm",
                        60301020304,
                        "NaN",
                        "CONSERV. PRÉDIOS ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020230Adm",
                        60301020230,
                        "ENERGIA ELETRICA",
                        "ENERGIA ELETRICA ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020210AAdm",
                        "60301020210A",
                        "COMBUSTIVEL GERADOR",
                        "COMB GERADOR ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020272Adm",
                        60301020272,
                        "COMBUSTIVEL GERADOR",
                        "COMB GERADOR ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020256Adm",
                        60301020256,
                        "LIMPEZA",
                        "LIMPEZA ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020202Adm",
                        60301020202,
                        "AGUA ",
                        "AGUA ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020201Adm",
                        60301020201,
                        "ALUGUEL DE EQUIPAMENTOS",
                        "ALUGUEL EQUIP ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020250Adm",
                        60301020250,
                        "SEGURO PREDIOS",
                        "SEGURO PREDIOS ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020245Adm",
                        60301020245,
                        "SEGURANCA PREDIAL",
                        "SEGURANCA PREDIAL ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301040104Adm",
                        60301040104,
                        "IPTU",
                        "IPTU ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020252Adm",
                        60301020252,
                        "TELEFONES",
                        "TELEFONES ADMN",
                        "INFORMATICA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020251Adm",
                        60301020251,
                        "PROVEDOR INTERNET",
                        "INTERNET ADMN",
                        "INFORMATICA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020233Adm",
                        60301020233,
                        "MAT. HIGIENE/LIMPEZA ",
                        "MAT. HIGIENE/LIMPEZA ADMN",
                        "ARMAZENAGEM ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020203Coml",
                        60301020203,
                        "ALUG. IMÓVEIS",
                        "ALUG. IMÓVEIS COML",
                        "ARMAZENAGEM OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020213Coml",
                        60301020213,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020214Coml",
                        60301020214,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020215Coml",
                        60301020215,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020287Coml",
                        60301020287,
                        "CONSERV. PRÉDIOS",
                        "CONSERV. PRÉDIOS COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020304Coml",
                        60301020304,
                        "NaN",
                        "CONSERV. PRÉDIOS COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020230Coml",
                        60301020230,
                        "ENERGIA ELETRICA",
                        "ENERGIA ELETRICA COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020210AComl",
                        "60301020210A",
                        "COMBUSTIVEL GERADOR",
                        "COMB GERADOR COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020272Coml",
                        60301020272,
                        "COMBUSTIVEL GERADOR",
                        "COMB GERADOR COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020256Coml",
                        60301020256,
                        "LIMPEZA",
                        "LIMPEZA COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020202Coml",
                        60301020202,
                        "AGUA ",
                        "AGUA COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020201Coml",
                        60301020201,
                        "ALUGUEL DE EQUIPAMENTOS",
                        "ALUGUEL EQUIP COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020250Coml",
                        60301020250,
                        "SEGURO PREDIOS",
                        "SEGURO PREDIOS COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020245Coml",
                        60301020245,
                        "SEGURANCA PREDIAL",
                        "SEGURANCA PREDIAL COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301040104Coml",
                        60301040104,
                        "IPTU",
                        "IPTU COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020252Coml",
                        60301020252,
                        "TELEFONES",
                        "TELEFONES COML",
                        "INFORMATICA COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020251Coml",
                        60301020251,
                        "PROVEDOR INTERNET",
                        "INTERNET COML",
                        "INFORMATICA COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020233Coml",
                        60301020233,
                        "MAT. HIGIENE/LIMPEZA ",
                        "MAT. HIGIENE/LIMPEZA COML",
                        "ARMAZENAGEM COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020275Oper",
                        60301020275,
                        "ASSESSORIA JURIDICA",
                        "ASSESS JURIDICA oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020284Oper",
                        60301020284,
                        "ASSESSORIA RH",
                        "ASSESS RH oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020276Oper",
                        60301020276,
                        "CERTIFICACAO/QUALIDA",
                        "QUALIDADE oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020280Oper",
                        60301020280,
                        "OUTROS SERVIÇOS TERCEIROS",
                        "ASSESS ADM oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020281Oper",
                        60301020281,
                        "MARKETING",
                        "MKT oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020283Oper",
                        60301020283,
                        "AUDITORIA / CONSULTORIA",
                        "AUDIT/CONSULT oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020282Oper",
                        60301020282,
                        "ASSESS. CONTABIL",
                        "ASSESS CONT oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020278Oper",
                        60301020278,
                        "OUTROS SERVIÇOS TERCEIROS",
                        "OUTROS 3S oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020223Oper",
                        60301020223,
                        "HONORÁRIOS PROF. DIVERSOS",
                        "HONO PROF. DIV. - PJ oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020224Oper",
                        60301020224,
                        "HONORÁRIOS PROF. DIVERSOS",
                        "HONO PROF. DIV. - PF oper",
                        "TERCEIROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020275Adm",
                        60301020275,
                        "ASSESSORIA JURIDICA",
                        "ASSESS JURIDICA admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020284Adm",
                        60301020284,
                        "ASSESSORIA RH",
                        "ASSESS RH admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020276Adm",
                        60301020276,
                        "CERTIFICACAO/QUALIDA",
                        "QUALIDADE admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020280Adm",
                        60301020280,
                        "OUTROS SERVIÇOS TERCEIROS",
                        "ASSESS ADM admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020281Adm",
                        60301020281,
                        "MARKETING",
                        "MKT admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020283Adm",
                        60301020283,
                        "AUDITORIA / CONSULTORIA",
                        "AUDIT/CONSULT admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020282Adm",
                        60301020282,
                        "ASSESS. CONTABIL",
                        "ASSESS CONT admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020278Adm",
                        60301020278,
                        "OUTROS SERVIÇOS TERCEIROS",
                        "OUTROS 3S admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020223Adm",
                        60301020223,
                        "HONORÁRIOS PROF. DIVERSOS",
                        "HONO PROF. DIV. - PJ admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020224Adm",
                        60301020224,
                        "HONORÁRIOS PROF. DIVERSOS",
                        "HONO PROF. DIV. - PF admn",
                        "TERCEIROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020275Coml",
                        60301020275,
                        "ASSESSORIA JURIDICA",
                        "ASSESS JURIDICA coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020284Coml",
                        60301020284,
                        "ASSESSORIA RH",
                        "ASSESS RH coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020276Coml",
                        60301020276,
                        "CERTIFICACAO/QUALIDA",
                        "QUALIDADE coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020280Coml",
                        60301020280,
                        "OUTROS SERVIÇOS TERCEIROS",
                        "ASSESS ADM coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020281Coml",
                        60301020281,
                        "MARKETING",
                        "MKT coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020283Coml",
                        60301020283,
                        "AUDITORIA / CONSULTORIA",
                        "AUDIT/CONSULT coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020282Coml",
                        60301020282,
                        "ASSESS. CONTABIL",
                        "ASSESS CONT coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020278Coml",
                        60301020278,
                        "OUTROS SERVIÇOS TERCEIROS",
                        "OUTROS 3S coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020223Coml",
                        60301020223,
                        "HONORÁRIOS PROF. DIVERSOS",
                        "HONO PROF. DIV. - PJ coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020224Coml",
                        60301020224,
                        "HONORÁRIOS PROF. DIVERSOS",
                        "HONO PROF. DIV. - PF coml",
                        "TERCEIROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020277Oper",
                        60301020277,
                        "ASSESS. E SERV INFOR",
                        "ASSESS. E SERV INFOR OPER",
                        "INFORMATICA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020231Oper",
                        60301020231,
                        "MANUT.SISTEM.INFOR",
                        "MANUT.SISTEM.INFOR OPER",
                        "INFORMATICA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020260Oper",
                        60301020260,
                        "MANUT.EQUIP.INFORMAT.",
                        "MANUT.EQUIP.INFOR OPER",
                        "INFORMATICA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020234Oper",
                        60301020234,
                        "MAT PROCES. DADOS",
                        "MAT PROCES. DADOS OPER",
                        "INFORMATICA OPER",
                        "Não Financeiro"
                    ],
                    [
                        "60301020277Adm",
                        60301020277,
                        "ASSESS. E SERV INFOR",
                        "ASSESS. E SERV INFOR ADMN",
                        "INFORMATICA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020231Adm",
                        60301020231,
                        "MANUT.SISTEM.INFOR",
                        "MANUT.SISTEM.INFOR ADMN",
                        "INFORMATICA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020260Adm",
                        60301020260,
                        "MANUT.EQUIP.INFORMAT.",
                        "MANUT.EQUIP.INFOR ADMN",
                        "INFORMATICA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020234Adm",
                        60301020234,
                        "MAT PROCES. DADOS",
                        "MAT PROCES. DADOS ADMN",
                        "INFORMATICA ADMN",
                        "Não Financeiro"
                    ],
                    [
                        "60301020277Coml",
                        60301020277,
                        "ASSESS. E SERV INFOR",
                        "ASSESS. E SERV INFOR COML",
                        "INFORMATICA COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020231Coml",
                        60301020231,
                        "MANUT.SISTEM.INFOR",
                        "MANUT.SISTEM.INFOR COML",
                        "INFORMATICA COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020260Coml",
                        60301020260,
                        "MANUT.EQUIP.INFORMAT.",
                        "MANUT.EQUIP.INFOR COML",
                        "INFORMATICA COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301020234Coml",
                        60301020234,
                        "MAT PROCES. DADOS",
                        "MAT PROCES. DADOS COML",
                        "INFORMATICA COML",
                        "Não Financeiro"
                    ],
                    [
                        "60301040106Oper",
                        60301040106,
                        "MULTAS DIVERSAS",
                        "MULTAS DIVERSAS oper",
                        "IMPOSTOS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020257Oper",
                        60301020257,
                        "TAXAS E CUSTAS",
                        "TAXAS E CUSTAS oper",
                        "IMPOSTOS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301040101Oper",
                        60301040101,
                        "TAXAS E CUSTAS",
                        "IMP ESTADUAL oper",
                        "IMPOSTOS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301040102Oper",
                        60301040102,
                        "TAXAS E CUSTAS",
                        "IMP FEDERAL oper",
                        "IMPOSTOS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301040103Oper",
                        60301040103,
                        "TAXAS E CUSTAS",
                        "IMP MUNICIPAL oper",
                        "IMPOSTOS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301040106Adm",
                        60301040106,
                        "MULTAS DIVERSAS",
                        "MULTAS DIVERSAS admn",
                        "IMPOSTOS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020257Adm",
                        60301020257,
                        "TAXAS E CUSTAS",
                        "TAXAS E CUSTAS admn",
                        "IMPOSTOS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301040101Adm",
                        60301040101,
                        "TAXAS E CUSTAS",
                        "IMP ESTADUAL admn",
                        "IMPOSTOS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301040102Adm",
                        60301040102,
                        "TAXAS E CUSTAS",
                        "IMP FEDERAL admn",
                        "IMPOSTOS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301040103Adm",
                        60301040103,
                        "TAXAS E CUSTAS",
                        "IMP MUNICIPAL admn",
                        "IMPOSTOS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301040106Coml",
                        60301040106,
                        "MULTAS DIVERSAS",
                        "MULTAS DIVERSAS coml",
                        "IMPOSTOS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020257Coml",
                        60301020257,
                        "TAXAS E CUSTAS",
                        "TAXAS E CUSTAS coml",
                        "IMPOSTOS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301040101Coml",
                        60301040101,
                        "TAXAS E CUSTAS",
                        "IMP ESTADUAL coml",
                        "IMPOSTOS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301040102Coml",
                        60301040102,
                        "TAXAS E CUSTAS",
                        "IMP FEDERAL coml",
                        "IMPOSTOS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301040103Coml",
                        60301040103,
                        "TAXAS E CUSTAS",
                        "IMP MUNICIPAL coml",
                        "IMPOSTOS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020205Oper",
                        60301020205,
                        "ANUNC/PUBLICAÇÕES",
                        "ANUNC/PUBLICAÇÕES oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020118Oper",
                        60301020118,
                        "SALARIOS",
                        "contrib. Sindical oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020207Oper",
                        60301020207,
                        "ASSOS/SIND.CLASSE",
                        "ASSOS/SIND.CLASSE oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020208Oper",
                        60301020208,
                        "ALUGUEL DE VEICULOS",
                        "BENS USO/CONSUMO oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020267Oper",
                        60301020267,
                        "BENS USO/CONSUMO",
                        "BENS USO/CONSUMO oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020268Oper",
                        60301020268,
                        "BRINDES E DOAÇÕES",
                        "BRINDES oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020211Oper",
                        60301020211,
                        "COND.E TRANSPORT",
                        "COND.E TRANSPORT oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020217Oper",
                        60301020217,
                        "CÓPIAS XEROGRÁF.",
                        "CÓPIAS oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020259Oper",
                        60301020259,
                        "DESP. DIVERSAS",
                        "DESP. DIVERSAS oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020220Oper",
                        60301020220,
                        "ESTACIONAMENTOS",
                        "ESTACIONA oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020296Oper",
                        60301020296,
                        "FEIRA E EVENTOS",
                        "FEIRA E EVENTOS oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020286Oper",
                        60301020286,
                        "GAS GLP - REFEITORIO",
                        "GAS GLP - REFEITORIO oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020225Oper",
                        60301020225,
                        "IMPRES. GRAFICOS",
                        "IMPRES. GRAFICOS oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020228Oper",
                        60301020228,
                        "JORNAIS/REVISTAS ",
                        "JORNAIS/REVISTAS oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020235Oper",
                        60301020235,
                        "MATERIAL DE EXPEDIENTE",
                        "MAT EXPEDIENTE oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020240Oper",
                        60301020240,
                        "PORTES/TELEGRAMA",
                        "PORTES/TELEGRAMA oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020242Oper",
                        60301020242,
                        "REFEIÇÕES ADM ",
                        "REFEIÇÕES ADM oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020254Oper",
                        60301020254,
                        "VIAGEM ",
                        "VIAGEM oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60601010102Oper",
                        60601010102,
                        "(+) Outras Rec Operac",
                        "OUTRAS REC/DESP OPERAC oper",
                        "OUTROS oper",
                        "Não Financeiro"
                    ],
                    [
                        "60301020205Adm",
                        60301020205,
                        "ANUNC/PUBLICAÇÕES",
                        "ANUNC/PUBLICAÇÕES admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020118Adm",
                        60301020118,
                        "SALARIOS",
                        "contrib. Sindical admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020207Adm",
                        60301020207,
                        "ASSOS/SIND.CLASSE",
                        "ASSOS/SIND.CLASSE admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020208Adm",
                        60301020208,
                        "ALUGUEL DE VEICULOS",
                        "BENS USO/CONSUMO admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020267Adm",
                        60301020267,
                        "BENS USO/CONSUMO",
                        "BENS USO/CONSUMO admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020268Adm",
                        60301020268,
                        "BRINDES E DOAÇÕES",
                        "BRINDES admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020211Adm",
                        60301020211,
                        "COND.E TRANSPORT",
                        "COND.E TRANSPORT admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020217Adm",
                        60301020217,
                        "CÓPIAS XEROGRÁF.",
                        "CÓPIAS admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020259Adm",
                        60301020259,
                        "DESP. DIVERSAS",
                        "DESP. DIVERSAS admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020220Adm",
                        60301020220,
                        "ESTACIONAMENTOS",
                        "ESTACIONA admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020296Adm",
                        60301020296,
                        "FEIRA E EVENTOS",
                        "FEIRA E EVENTOS admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020286Adm",
                        60301020286,
                        "GAS GLP - REFEITORIO",
                        "GAS GLP - REFEITORIO admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020225Adm",
                        60301020225,
                        "IMPRES. GRAFICOS",
                        "IMPRES. GRAFICOS admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020228Adm",
                        60301020228,
                        "JORNAIS/REVISTAS ",
                        "JORNAIS/REVISTAS admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020235Adm",
                        60301020235,
                        "MATERIAL DE EXPEDIENTE",
                        "MAT EXPEDIENTE admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020240Adm",
                        60301020240,
                        "PORTES/TELEGRAMA",
                        "PORTES/TELEGRAMA admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020242Adm",
                        60301020242,
                        "REFEIÇÕES ADM ",
                        "REFEIÇÕES ADM admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020254Adm",
                        60301020254,
                        "VIAGEM ",
                        "VIAGEM admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60601010102Adm",
                        60601010102,
                        "(+) Outras Rec admnac",
                        "OUTRAS REC/DESP admnAC admn",
                        "OUTROS admn",
                        "Não Financeiro"
                    ],
                    [
                        "60301020205Coml",
                        60301020205,
                        "ANUNC/PUBLICAÇÕES",
                        "ANUNC/PUBLICAÇÕES coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020118Coml",
                        60301020118,
                        "SALARIOS",
                        "contrib. Sindical coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020207Coml",
                        60301020207,
                        "ASSOS/SIND.CLASSE",
                        "ASSOS/SIND.CLASSE coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020208Coml",
                        60301020208,
                        "ALUGUEL DE VEICULOS",
                        "BENS USO/CONSUMO coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020267Coml",
                        60301020267,
                        "BENS USO/CONSUMO",
                        "BENS USO/CONSUMO coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020268Coml",
                        60301020268,
                        "BRINDES E DOAÇÕES",
                        "BRINDES coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020211Coml",
                        60301020211,
                        "COND.E TRANSPORT",
                        "COND.E TRANSPORT coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020217Coml",
                        60301020217,
                        "CÓPIAS XEROGRÁF.",
                        "CÓPIAS coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020259Coml",
                        60301020259,
                        "DESP. DIVERSAS",
                        "DESP. DIVERSAS coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020220Coml",
                        60301020220,
                        "ESTACIONAMENTOS",
                        "ESTACIONA coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020296Coml",
                        60301020296,
                        "FEIRA E EVENTOS",
                        "FEIRA E EVENTOS coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020286Coml",
                        60301020286,
                        "GAS GLP - REFEITORIO",
                        "GAS GLP - REFEITORIO coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020225Coml",
                        60301020225,
                        "IMPRES. GRAFICOS",
                        "IMPRES. GRAFICOS coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020228Coml",
                        60301020228,
                        "JORNAIS/REVISTAS ",
                        "JORNAIS/REVISTAS coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020235Coml",
                        60301020235,
                        "MATERIAL DE EXPEDIENTE",
                        "MAT EXPEDIENTE coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020240Coml",
                        60301020240,
                        "PORTES/TELEGRAMA",
                        "PORTES/TELEGRAMA coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020242Coml",
                        60301020242,
                        "REFEIÇÕES ADM ",
                        "REFEIÇÕES ADM coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60301020254Coml",
                        60301020254,
                        "VIAGEM ",
                        "VIAGEM coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "60601010102Coml",
                        60601010102,
                        "(+) Outras Rec comlac",
                        "OUTRAS REC/DESP comlAC coml",
                        "OUTROS coml",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60301020218,
                        "(-) Depreciações / Amortizações",
                        "DEPREC/AMORT",
                        "DEPREC/AMORT",
                        "DEPREC/AMORT"
                    ],
                    [
                        "NaN",
                        60301020266,
                        "(-) Depreciações / Amortizações",
                        "DEPREC/AMORT",
                        "DEPREC/AMORT",
                        "DEPREC/AMORT"
                    ],
                    [
                        "NaN",
                        60301020303,
                        "(-) Depreciações / Amortizações",
                        "DEPREC/AMORT",
                        "DEPREC/AMORT",
                        "DEPREC/AMORT"
                    ],
                    [
                        "NaN",
                        60301050102,
                        "JUROS PAGOS ",
                        "JUROS PAGOS",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60301050103,
                        "DESP. BANCÁRIAS",
                        "DESP. BANCÁRIAS",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60301050101,
                        "JUROS S/FINANCIAM.",
                        "JUROS S/FINANC",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60301050104,
                        "IOF",
                        "IOF",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        "NaN",
                        "CPMF",
                        "CPMF",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60301050107,
                        "VARIAÇÃO CAMBIAL",
                        "VAR CAMBIAL",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        "NaN",
                        "VARIAÇAO MON. PASSIVA",
                        "VAR MON. PASSIVA",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60301050202,
                        "JUROS AUFERIDOS",
                        "JUROS AUFERIDOS",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60301050203,
                        "RECEITAS APL. FINAN",
                        "RECEITAS APL. FINAN",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60301050204,
                        "DESC.AUFERIDOS",
                        "DESC.AUFERIDOS",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        "NaN",
                        "RESSARC. DESP. BANCAR",
                        "RESSARC. DESP. BANCAR",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        "NaN",
                        "VARIAÇAO MON. ATIVA",
                        "VAR MON. ATIVA",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        "NaN",
                        "BAIXA BENS AT.IMOB",
                        "BAIXA ATIVO IMOB",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60601020104,
                        "RECEITA VENDA IMOBIL",
                        "RECEITA VENDA IMOBIL",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60301040107,
                        "MULTAS/JUROS S/TRIB. ",
                        "MULTAS/JUROS S/TRIB. ",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60301050109,
                        "JUROS S/CAPITAL DE GIRO",
                        "JUROS S/CAPITAL DE GIRO",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ],
                    [
                        "NaN",
                        60601020101,
                        "NaN",
                        "BAIXAS ATIVO PERMANENTE",
                        "CUSTOS FINANCEIROS",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        "60301020283B",
                        "(-) Receitas / Despesas não Operacionais",
                        "REC/DESP NÃO OPERAC",
                        "REC/DESP NÃO OPERAC",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60000000000,
                        "(-) Receitas / Despesas não Operacionais",
                        "OUTROS AJUSTES",
                        "OUTROS AJUSTES",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60000000001,
                        "(-) Receitas / Despesas não Operacionais",
                        "OUTROS AJUSTES",
                        "OUTROS AJUSTES",
                        "Não Financeiro"
                    ],
                    [
                        "NaN",
                        60301050206,
                        "ATUALIZACAO MONETARIA PER/DCOMP",
                        "VLR REF. ATUALIZACAO MONETARIA CREDITO IINSS",
                        "CUSTOS FINANCEIROS",
                        "CUSTOS FINANCEIROS"
                    ]
                ]
            },
            "Volumes_De_Para_Abreviacao": {
                "index": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    24,
                    25,
                    26,
                    27,
                    28,
                    29,
                    30,
                    31,
                    32,
                    33,
                    34,
                    35,
                    36,
                    37,
                    38,
                    39,
                    40,
                    41,
                    42,
                    43,
                    44,
                    45,
                    46,
                    47,
                    48
                ],
                "columns": [
                    "Area",
                    "Grupo"
                ],
                "data": [
                    [
                        "ABL",
                        "ABL"
                    ],
                    [
                        "AST",
                        "ASTELLAS"
                    ],
                    [
                        "AZA",
                        "ASTRAZENECA"
                    ],
                    [
                        "BDC",
                        "BALDACCI"
                    ],
                    [
                        "BIO",
                        "NAOS BRASIL"
                    ],
                    [
                        "BLA",
                        "BLANVER"
                    ],
                    [
                        "BLS",
                        "BLANVER"
                    ],
                    [
                        "CAP",
                        "CAPSUGEL"
                    ],
                    [
                        "CYG",
                        "CYG BIOTECH"
                    ],
                    [
                        "DLT",
                        "CELLERA"
                    ],
                    [
                        "ECP",
                        "EXPANSCIENCE"
                    ],
                    [
                        "ESC",
                        "EXPANSCIENCE"
                    ],
                    [
                        "FAN",
                        "MARTIN BAUER"
                    ],
                    [
                        "GDI",
                        "GALDERMA"
                    ],
                    [
                        "GDS",
                        "GALDERMA"
                    ],
                    [
                        "GIL",
                        "GILEAD"
                    ],
                    [
                        "GNT",
                        "GRUNENTHAL"
                    ],
                    [
                        "GSH",
                        "GALDERMA"
                    ],
                    [
                        "IDI",
                        "ISDIN"
                    ],
                    [
                        "IPS",
                        "IPSEN"
                    ],
                    [
                        "ISD",
                        "ISDIN"
                    ],
                    [
                        "LDB",
                        "LUNDBECK"
                    ],
                    [
                        "LDD",
                        "MEGALABS"
                    ],
                    [
                        "LPH",
                        "LEO PHARMA"
                    ],
                    [
                        "MBB",
                        "MAWDSLEYS"
                    ],
                    [
                        "MER",
                        "MERZ"
                    ],
                    [
                        "MIP",
                        "CELLERA"
                    ],
                    [
                        "MJN",
                        "MEAD JOHNSON"
                    ],
                    [
                        "MK8",
                        "MOKSHA8"
                    ],
                    [
                        "MOK",
                        "MOKSHA8"
                    ],
                    [
                        "MOL",
                        "MOLKEM"
                    ],
                    [
                        "NDI",
                        "TAKEDA"
                    ],
                    [
                        "NVA",
                        "SEMINA"
                    ],
                    [
                        "PBN",
                        "PBN - QUÌMICA"
                    ],
                    [
                        "RBG",
                        "RANBAXY"
                    ],
                    [
                        "RBX",
                        "RANBAXY"
                    ],
                    [
                        "RKT",
                        "RECKITT"
                    ],
                    [
                        "SEI",
                        "SEMINA"
                    ],
                    [
                        "SEM",
                        "SEMINA"
                    ],
                    [
                        "SFB",
                        "SUN PHARMA"
                    ],
                    [
                        "SUN",
                        "SUNSTAR"
                    ],
                    [
                        "TAK",
                        "TAKEDA"
                    ],
                    [
                        "THS",
                        "THERAMEX"
                    ],
                    [
                        "TKD",
                        "TAKEDA"
                    ],
                    [
                        "UCB",
                        "UCB"
                    ],
                    [
                        "UNI",
                        "UNICHEM"
                    ],
                    [
                        "THX",
                        "THERAMEX"
                    ],
                    [
                        "EXE",
                        "EXELTIS"
                    ],
                    [
                        "RQT",
                        "ROQUETTE"
                    ]
                ]
            },
            "Embalagens_De_Para_Clientes": {
                "index": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    24,
                    25,
                    26,
                    27,
                    28,
                    29,
                    30,
                    31,
                    32,
                    33,
                    34,
                    35,
                    36,
                    37,
                    38,
                    39,
                    40,
                    41,
                    42,
                    43,
                    44,
                    45,
                    46
                ],
                "columns": [
                    "Abreviação",
                    "Nome Cliente",
                    "Filial UF"
                ],
                "data": [
                    [
                        "UNI",
                        "UNICHEM FARMACEUTICA DO BRASIL LTDA - SC",
                        "SC"
                    ],
                    [
                        "UCB",
                        "UCB BIOPHARMA S.A",
                        "SC"
                    ],
                    [
                        "TKD",
                        "TAKEDA DISTRIBUIDORA LTDA.",
                        "SP"
                    ],
                    [
                        "THS",
                        "THERAMEX FARMACEUTICA LTDA. /SC",
                        "SC"
                    ],
                    [
                        "SUN",
                        "SUNSTAR BRASIL IMPORT. E DISTRIB. LTDA",
                        "SP"
                    ],
                    [
                        "SFB",
                        "SUN FARMACEUTICA DO BRASIL LTDA - RJ",
                        "RJ"
                    ],
                    [
                        "RBX",
                        "RANBAXY FARMACEUTICA LTDA",
                        "RJ"
                    ],
                    [
                        "PBN",
                        "PBN QUIMICA E FARMACEUTICA LTDA",
                        "SP"
                    ],
                    [
                        "NDI",
                        "TAKEDA SANTA CATARINA",
                        "SC"
                    ],
                    [
                        "MOL",
                        "MOLKEM BRASIL LTDA",
                        "SP"
                    ],
                    [
                        "MK8",
                        "MOKSHA8 BRASIL INDÚSTRIA E COMÉRCIO DE MEDICAMENTOS LTDA/SC",
                        "SC"
                    ],
                    [
                        "MJN",
                        "MJN - MEAD JOHNSON DO BRASIL - SC",
                        "SC"
                    ],
                    [
                        "MIP",
                        "MIP - CELLERA CONSUMO LTDA",
                        "SP"
                    ],
                    [
                        "MER",
                        "MERZ FARMACEUTICA COMERCIAL LTDA.",
                        "SC"
                    ],
                    [
                        "MBB",
                        "MBB - MAWDSLEYS PHARMACEUTICALS DO BRASIL LTDA",
                        "SC"
                    ],
                    [
                        "LPH",
                        "LEO PHARMA LTDA",
                        "SP"
                    ],
                    [
                        "LDD",
                        "MEGALABS FARMACÊUTICA S.A",
                        "RJ"
                    ],
                    [
                        "LDB",
                        "LUNDBECK BRASIL LTDA - FILIAL",
                        "RJ"
                    ],
                    [
                        "ISD",
                        "ISDIN PRODUTOS FARMACEUTICOS LTDA - SP",
                        "SP"
                    ],
                    [
                        "IPS",
                        "BEAUFOUR IPSEN FARMACEUTICA LTDA/SC",
                        "SC"
                    ],
                    [
                        "IDI",
                        "ISDIN PRODUTOS FARMACÊUTICOS LTDA - SC",
                        "SC"
                    ],
                    [
                        "GNT",
                        "GRUNENTHAL DO BRASIL FARMACEUTICA LTDA / SC",
                        "SC"
                    ],
                    [
                        "GDS",
                        "GAL - GALDERMA BRASIL LTDA SP",
                        "SP"
                    ],
                    [
                        "FAN",
                        "FINZELBERG ATIVOS NATURAIS LTDA",
                        "SP"
                    ],
                    [
                        "ESC",
                        "LABORATORIOS EXPANSCIENCE DO BRASIL - SC",
                        "SC"
                    ],
                    [
                        "ECP",
                        "LABORATORIOS EXPANSCIENCE DO BRASIL - SP",
                        "SP"
                    ],
                    [
                        "DLT",
                        "CELLERA FARMACEUTICA S.A.",
                        "SP"
                    ],
                    [
                        "CYG",
                        "CYG BIOTECH QUIMICA & FARMACEUTICA LTDA",
                        "SP"
                    ],
                    [
                        "CAP",
                        "CAPSUGEL BRASIL IMPORT. DISTR. DE INSUMOS FARM. E ALIM. LTDA",
                        "SC"
                    ],
                    [
                        "BLS",
                        "BLANVER FARMOQUIMICA E FARMACEUTICA S.A. - SC",
                        "SC"
                    ],
                    [
                        "BLA",
                        "BLANVER FARMOQUIMICA E FARMACEUTICA S.A. - SP",
                        "SP"
                    ],
                    [
                        "BIO",
                        "LABORATORIOS BIODERMA DO BRASIL LTDA",
                        "SP"
                    ],
                    [
                        "AZA",
                        "ASTRAZENECA DO BRASIL LTDA",
                        "SP"
                    ],
                    [
                        "AST",
                        "ASTELLAS FARMA BRASIL IMPORTACAO E DIST. DE MED. LTDA.",
                        "SP"
                    ],
                    [
                        "ABL",
                        "ABL - ANTIBIOTICOS DO BRASIL LTDA",
                        "SC"
                    ],
                    [
                        "THX",
                        "THERAMEX FARMACEUTICA LTDA /SP",
                        "SP"
                    ],
                    [
                        "TAK",
                        "TAKEDA PHARMA LTDA",
                        "SP"
                    ],
                    [
                        "RBG",
                        "RANBAXY FARMACEUTICA LTDA - GO",
                        "GO"
                    ],
                    [
                        "GSH",
                        "GSH - GALDERMA DISTRIBUIDORA DO BRASIL LTDA - SC",
                        "SC"
                    ],
                    [
                        "GIL",
                        "GILEAD SCIENCES FARMACEUTICA DO BRASIL LTDA",
                        "SC"
                    ],
                    [
                        "SEM",
                        "SEMINA INDÚSTRIA E COMÉRCIO LTDA",
                        "SP"
                    ],
                    [
                        "SEI",
                        "SEMINA INSUMOS ESTRATEGICOS LTDA",
                        "SP"
                    ],
                    [
                        "NVA",
                        "NVAA COM. DE PRODUTOS DE PERFUMARIA E HIGIENE PESSOAL LTDA",
                        "SP"
                    ],
                    [
                        "MOK",
                        "MOKSHA8 BRASIL INDÚSTRIA E COMÉRCIO DE MEDICAMENTOS LTDA/SP",
                        "SP"
                    ],
                    [
                        "GDI",
                        "GALDERMA BRASIL LTDA SC",
                        "SC"
                    ],
                    [
                        "BDC",
                        "LABORATORIOS BALDACCI LTDA",
                        "SP"
                    ],
                    [
                        "RQT",
                        "ITACEL FARMOQUIMICA LTDA",
                        "SP"
                    ]
                ]
            },
            "MO_Ade_Temp_Filial_UF": {
                "index": [
                    0,
                    1
                ],
                "columns": [
                    "Filial",
                    "Filial UF"
                ],
                "data": [
                    [
                        "Itajaí",
                        "SC"
                    ],
                    [
                        "Itapevi",
                        "SP"
                    ]
                ]
            },
            "MO_Ade_Temp_Cli_Grupo": {
                "index": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6
                ],
                "columns": [
                    "Cliente",
                    "Grupo"
                ],
                "data": [
                    [
                        "Expancience",
                        "EXPANSCIENCE"
                    ],
                    [
                        "Leo Pharma",
                        "LEO PHARMA"
                    ],
                    [
                        "Galderma",
                        "GALDERMA"
                    ],
                    [
                        "Merz",
                        "MERZ"
                    ],
                    [
                        "Sunstar",
                        "SUNSTAR"
                    ],
                    [
                        "MIP",
                        "CELLERA"
                    ],
                    [
                        "Reckitt",
                        "RECKITT"
                    ]
                ]
            },
            "Item_De_Para_Filial_Depreciacao": {
                "index": [
                    0,
                    1,
                    2,
                    3,
                    4,
                    5,
                    6,
                    7,
                    8,
                    9,
                    10,
                    11,
                    12,
                    13,
                    14,
                    15,
                    16,
                    17,
                    18,
                    19,
                    20,
                    21,
                    22,
                    23,
                    24,
                    25,
                    26,
                    27,
                    28,
                    29,
                    30,
                    31,
                    32,
                    33,
                    34,
                    35,
                    36,
                    37,
                    38,
                    39,
                    40,
                    41,
                    42,
                    43,
                    44,
                    45,
                    46,
                    47,
                    48
                ],
                "columns": [
                    "Item",
                    "Filial UF"
                ],
                "data": [
                    [
                        11002,
                        "SC"
                    ],
                    [
                        10302,
                        "SP"
                    ],
                    [
                        10196,
                        "SP"
                    ],
                    [
                        10323,
                        "SP"
                    ],
                    [
                        10802,
                        "GO"
                    ],
                    [
                        10363,
                        "SP"
                    ],
                    [
                        10702,
                        "RJ"
                    ],
                    [
                        10399007,
                        "SP"
                    ],
                    [
                        10154,
                        "SP"
                    ],
                    [
                        10195,
                        "SP"
                    ],
                    [
                        10197,
                        "SC"
                    ],
                    [
                        10101,
                        "SP"
                    ],
                    [
                        10102,
                        "SP"
                    ],
                    [
                        10152,
                        "SP"
                    ],
                    [
                        10153,
                        "SP"
                    ],
                    [
                        10155,
                        "SP"
                    ],
                    [
                        10172,
                        "RJ"
                    ],
                    [
                        10399037,
                        "SP"
                    ],
                    [
                        10719,
                        "RJ"
                    ],
                    [
                        11008,
                        "SC"
                    ],
                    [
                        11202,
                        "DESC"
                    ],
                    [
                        11203,
                        "DESC"
                    ],
                    [
                        11502,
                        "DESC"
                    ],
                    [
                        10110,
                        "SP"
                    ],
                    [
                        10181,
                        "SP"
                    ],
                    [
                        10185,
                        "SP"
                    ],
                    [
                        10187,
                        "SC"
                    ],
                    [
                        10191,
                        "SP"
                    ],
                    [
                        10198,
                        "SP"
                    ],
                    [
                        10203,
                        "SP"
                    ],
                    [
                        10327,
                        "SP"
                    ],
                    [
                        10399011,
                        "SP"
                    ],
                    [
                        10399020,
                        "SP"
                    ],
                    [
                        10399030,
                        "SP"
                    ],
                    [
                        10706,
                        "RJ"
                    ],
                    [
                        1100303,
                        "SP"
                    ],
                    [
                        11004,
                        "SC"
                    ],
                    [
                        11005,
                        "SC"
                    ],
                    [
                        11007,
                        "SC"
                    ],
                    [
                        11013,
                        "SC"
                    ],
                    [
                        11022,
                        "SC"
                    ],
                    [
                        11031,
                        "SC"
                    ],
                    [
                        11032,
                        "SC"
                    ],
                    [
                        11039,
                        "SC"
                    ],
                    [
                        11105,
                        "SP"
                    ],
                    [
                        10708,
                        "RJ"
                    ],
                    [
                        10106,
                        "SP"
                    ],
                    [
                        11604,
                        "SP"
                    ],
                    [
                        10190,
                        "DESC"
                    ]
                ]
            },
            "De_Para_Grupos_Ocupacao":{
            "columns":[
                "Cliente",
                "Area",
                "Item",
                "Grupo",
                "Filial"
            ],
            "index":[
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
                21,
                22,
                23,
                24,
                25,
                26,
                27,
                28,
                29,
                30,
                31,
                32,
                33,
                34,
                35,
                36,
                37,
                38,
                39,
                40,
                41,
                42,
                43,
                44,
                45,
                46,
                47,
                48,
                49,
                50,
                51,
                52,
                53,
                54
            ],
            "data":[
                [
                    "ABL",
                    "ABL - ANTIBIOTICOS DO BRASIL LTDA",
                    "ABL",
                    "ABL",
                    "SC"
                ],
                [
                    "ASTELLAS",
                    "ASTELLAS FARMA BRASIL IMPORTACAO E DIST. DE MED. LTDA.",
                    "AST",
                    "ASTELLAS",
                    "SP"
                ],
                [
                    "AZTRAZENECA",
                    "ASTRAZENECA DO BRASIL LTDA",
                    "AZA",
                    "ASTRAZENECA",
                    "SP"
                ],
                [
                    "BALDACCI",
                    "LABORATORIOS BALDACCI LTDA",
                    "BDC",
                    "BALDACCI",
                    "SP"
                ],
                [
                    "BLANVER  FARMOQUIMICA",
                    "BLANVER FARMOQUIMICA E FARMACEUTICA S.A. - SC",
                    "BLS",
                    "BLANVER",
                    "SC"
                ],
                [
                    "BLANVER  FARMOQUIMICA",
                    "BLANVER FARMOQUIMICA E FARMACEUTICA S.A. - SC",
                    "BLA",
                    "BLANVER",
                    "SP"
                ],
                [
                    "BLANVER FARMOQUIMICA",
                    "BLANVER FARMOQUIMICA E FARMACEUTICA S.A. - SC",
                    "BLA",
                    "BLANVER",
                    "SP"
                ],
                [
                    "BLANVER FARMOQUIMICA",
                    "BLANVER FARMOQUIMICA E FARMACEUTICA S.A. - SC",
                    "BLA",
                    "BLANVER",
                    "SC"
                ],
                [
                    "CAPSUGEL",
                    "CAPSUGEL BRASIL IMPORT. DISTR. DE INSUMOS FARM. E ALIM. LTDA",
                    "CAP",
                    "CAPSUGEL",
                    "SC"
                ],
                [
                    "CELLERA CONSUMO",
                    "MIP - CELLERA CONSUMO LTDA",
                    "MIP",
                    "CELLERA",
                    "SP"
                ],
                [
                    "CELLERA FARMA",
                    "CELLERA FARMACEUTICA S.A.",
                    "DLT",
                    "CELLERA",
                    "SP"
                ],
                [
                    "CYG BIOTECH",
                    "CYG BIOTECH QUIMICA & FARMACEUTICA LTDA",
                    "null",
                    "CYG BIOTECH",
                    "SP"
                ],
                [
                    "EXPANSCIENCE",
                    "LABORATORIOS EXPANSCIENCE DO BRASIL - SC",
                    "ESC",
                    "EXPANSCIENCE",
                    "SC"
                ],
                [
                    "EXPANSCIENCE",
                    "LABORATORIOS EXPANSCIENCE DO BRASIL - SP",
                    "ECP",
                    "EXPANSCIENCE",
                    "SP"
                ],
                [
                    "FINZELBERG",
                    "FINZELBERG ATIVOS NATURAIS LTDA",
                    "FAN",
                    "MARTIN BAUER",
                    "SP"
                ],
                [
                    "GALDERMA",
                    "GAL - GALDERMA DISTRIBUIDORA DO BRASIL LTDA - SP",
                    "GDS",
                    "GALDERMA",
                    "SP"
                ],
                [
                    "GALDERMA BRASIL ",
                    "GALDERMA BRASIL LTDA",
                    "GDI",
                    "GALDERMA",
                    "SC"
                ],
                [
                    "GALDERMA DISTR ",
                    "GALDERMA BRASIL LTDA",
                    "GDI",
                    "GALDERMA",
                    "SC"
                ],
                [
                    "GILEAD",
                    "GILEAD SCIENCES FARMACEUTICA DO BRASIL LTDA",
                    "GILEAD",
                    "GILEAD",
                    "SC"
                ],
                [
                    "GRUNENTHAL ",
                    "GRUNENTHAL DO BRASIL FARMACEUTICA LTDA \/ SC",
                    "GNT",
                    "GRUNENTHAL",
                    "SC"
                ],
                [
                    "IPSEN",
                    "BEAUFOUR IPSEN FARMACEUTICA LTDA\/SC",
                    "IPS",
                    "IPSEN",
                    "SC"
                ],
                [
                    "IPSEN",
                    "IPSP - BEAUFOUR IPSEN FARMACEUTICA LTDA \/SP",
                    "IPSP",
                    "IPSEN",
                    "SP"
                ],
                [
                    "ISDIN",
                    "ISDIN PRODUTOS FARMACÃŠUTICOS LTDA - SC",
                    "IDI",
                    "ISDIN",
                    "SC"
                ],
                [
                    "ISDIN",
                    "ISDIN PRODUTOS FARMACÃŠUTICOS LTDA - SP",
                    "ISD",
                    "ISDIN",
                    "SP"
                ],
                [
                    "LEO PHARMA",
                    "null",
                    "LPH",
                    "LEO PHARMA",
                    "SP"
                ],
                [
                    "LONZA\/CSG",
                    "CAPSUGEL BRASIL IMPORT. DISTR. DE INSUMOS FARM. E ALIM. LTDA",
                    "CAP",
                    "CAPSUGEL",
                    "SC"
                ],
                [
                    "LUNDBECK ",
                    "LUNDBECK BRASIL LTDA",
                    "LDB",
                    "LUNDBECK",
                    "RJ"
                ],
                [
                    "MAWDSLEYS",
                    "MBB - MAWDSLEYS PHARMACEUTICALS DO BRASIL LTDA",
                    "MBB",
                    "MARTIN BAUER",
                    "SC"
                ],
                [
                    "MEAD JOHNSON",
                    "MJN - MEAD JOHNSON DO BRASIL - SC",
                    "MJN",
                    "MEAD JOHNSON",
                    "SC"
                ],
                [
                    "MEGALABS",
                    "MEGALABS FARMACÃŠUTICA S.A",
                    "LDD",
                    "MEGALABS",
                    "RJ"
                ],
                [
                    "MERZ ",
                    "MERZ FARMACEUTICA COMERCIAL LTDA.",
                    "MER",
                    "MERZ",
                    "SC"
                ],
                [
                    "MOKSHA ",
                    "MOKSHA8 BRASIL INDÃšSTRIA E COMÃ‰RCIO DE MEDICAMENTOS LTDA\/SC",
                    "MK8",
                    "MOKSHA8",
                    "SC"
                ],
                [
                    "MOKSHA ",
                    "MOKSHA8 BRASIL INDÃšSTRIA E COMÃ‰RCIO DE MEDICAMENTOS LTDA\/SP",
                    "MOK",
                    "MOKSHA8",
                    "SP"
                ],
                [
                    "MOLKEM",
                    "MOLKEM BRASIL LTDA",
                    "MOL",
                    "MOLKEM",
                    "SP"
                ],
                [
                    "NAOS \/ BIODERMA",
                    "LABORATORIOS BIODERMA DO BRASIL LTDA",
                    "BIO",
                    "NAOS BRASIL",
                    "SP"
                ],
                [
                    "NVAA - SEMINA ",
                    "NVAA COM. DE PRODUTOS DE PERFUMARIA E HIGIENE PESSOAL LTDA",
                    "SEI",
                    "SEMINA",
                    "SP"
                ],
                [
                    "PBN QUIMICA",
                    "PBN QUIMICA E FARMACEUTICA LTDA",
                    "PBN",
                    "PBN - QUÌMICA",
                    "SP"
                ],
                [
                    "RANBAXY ",
                    "RANBAXY FARMACEUTICA LTDA",
                    "RBX",
                    "RANBAXY",
                    "RJ"
                ],
                [
                    "RANBAXY ",
                    "RANBAXY FARMACEUTICA LTDA - GO",
                    "RBG",
                    "RANBAXY",
                    "GO"
                ],
                [
                    "RECKITT",
                    "null",
                    "RKT",
                    "RECKITT",
                    "SP"
                ],
                [
                    "ROQUETE",
                    "null",
                    "null",
                    "ROQUETE",
                    "SP"
                ],
                [
                    "SEMINA ",
                    "SEMINA INDÃšSTRIA E COMÃ‰RCIO LTDA",
                    "SEM",
                    "SEMINA",
                    "SP"
                ],
                [
                    "SEMINA INSUMOS ",
                    "SEMINA INSUMOS ESTRATEGICOS LTDA",
                    "SEI",
                    "SEMINA",
                    "SP"
                ],
                [
                    "SUN FARMA",
                    "SUN FARMACEUTICA DO BRASIL LTDA",
                    "SFB",
                    "SUN PHARMA",
                    "RJ"
                ],
                [
                    "SUN FARMA",
                    "SUN FARMACEUTICA DO BRASIL LTDA - GO",
                    "SFG",
                    "SUN PHARMA",
                    "GO"
                ],
                [
                    "SUN FARMA",
                    "SUN FARMACEUTICA DO BRASIL LTDA - GO",
                    "SFG",
                    "SUN PHARMA",
                    "SP"
                ],
                [
                    "SUNSTAR ",
                    "SUNSTAR BRASIL IMPORT. E DISTRIB. LTDA",
                    "SUN",
                    "SUNSTAR",
                    "SP"
                ],
                [
                    "TAKEDA  DISTRIBUIDORA",
                    "null",
                    "null",
                    "TAKEDA",
                    "SP"
                ],
                [
                    "TAKEDA DISTRIBUIDORA",
                    "TAKEDA DISTRIBUIDORA",
                    "TKI",
                    "TAKEDA",
                    "SP"
                ],
                [
                    "TAKEDA PHARMA",
                    "null",
                    "null",
                    "TAKEDA",
                    "SP"
                ],
                [
                    "TAKEDA SC ",
                    "TAKEDA SANTA CATARINA",
                    "TAK",
                    "TAKEDA",
                    "SC"
                ],
                [
                    "THERAMEX",
                    "THERAMEX FARMACEUTICA LTDA. \/SC",
                    "THS",
                    "THERAMEX",
                    "SC"
                ],
                [
                    "THERAMEX",
                    "THERAMEX FARMACEUTICA LTDA \/SP",
                    "THX",
                    "THERAMEX",
                    "SP"
                ],
                [
                    "UCB",
                    "UCB BIOPHARMA S.A",
                    "UCB",
                    "UCB",
                    "SC"
                ],
                [
                    "UNICHEM ",
                    "UNICHEM FARMACEUTICA DO BRASIL LTDA",
                    "UNI",
                    "UNICHEM",
                    "SC"
                ]]
            }
        }
    }
}  
            with open(Json.caminho_json_rentabilidade_armazem_dados, "w", encoding="utf-8") as f:
                    json.dump(json_padrao_rentabilidade_de_para, f, indent=4)
            return json_padrao_rentabilidade_de_para
        else:
            with open(Json.caminho_json_rentabilidade_armazem_dados, "w", encoding="utf-8") as f:
                    json.dump(json_padrao_rentabilidade_de_para, f, indent=4)
            return json_padrao_rentabilidade_de_para                  

    @staticmethod
    def carregar_json_interativo_rentabilidade_armazem() -> dict:
        if not os.path.exists(Json.caminho_json_rentabilidade_armazem_arquivos):
            st.warning("Arquivo JSON não encontrado. Criando com valores padrão...")
            return Json_Rentabilidade.gerar_json_padrao_rentabilidade()
        
        with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r",encoding="utf-8") as f:
            caminhos_rentabilidade = json.load(f)

        if not os.path.exists(Json.caminho_json_rentabilidade_armazem_dados):
            st.warning("Arquivo JSON do De_Para não encontrado. Criando com valores padrão...")
            Json_Rentabilidade.gerar_json_padrao_rentabilidade_de_para()

        caminhos_invalidos_rentabilidade = {
            nome: info for nome, info in caminhos_rentabilidade.items()
            if isinstance(info, dict) and "path" in info and not os.path.exists(info["path"])
        }

        
        if caminhos_invalidos_rentabilidade:
            st.error("Alguns caminhos de arquivos não foram encontrados:")

            novos_caminhos_rentabilidade = {}

            table_rentabilidade,table_rentabilidade_2 = st.columns(2)

            for nome, info in caminhos_invalidos_rentabilidade.items():
                with table_rentabilidade:
                    st.text(f"{nome} (atual): {info['path']}")
                    if nome == "De_Para":
                        if st.button("Clique para gerar o Arquivo de De-Para Padrão"):
                            with open(Json.caminho_json_rentabilidade_armazem_dados, "r", encoding="utf-8") as f:
                                caminhos_rentabilidade_de_para = json.load(f)
                            
                            abas = caminhos_rentabilidade_de_para.get("De_Para", {}).get("sheet_name", {})

                            with pd.ExcelWriter(info['path'], engine='xlsxwriter') as writer:
                                for aba, dados in abas.items():
                                    if isinstance(dados, dict) and {'columns', 'data'}.issubset(dados):
                                        df = pd.DataFrame(data=dados['data'], columns=dados['columns'])
                                    else:
                                        df = pd.DataFrame(dados)
                                    df.to_excel(writer, sheet_name=aba[:31], index=False)

                            st.success("Arquivo de De-Para gerado com sucesso.")

                    with table_rentabilidade_2:    
                        novo = st.text_input(f"Informe o novo path para '{nome}':", key=f"input_{nome}")
                        if novo:
                            novos_caminhos_rentabilidade[nome] = novo

                if st.button("Salvar novos caminhos"):
                    for nome, novo_path in novos_caminhos_rentabilidade.items():
                        caminhos_rentabilidade[nome]["path"] = novo_path

                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as f:
                        json.dump(caminhos_rentabilidade, f, indent=4)

                    st.success("JSON atualizado com sucesso. Recarregue a aplicação.")
                    st.stop()
            return caminhos_rentabilidade
        return caminhos_rentabilidade

    @staticmethod
    def gerar_arquivo_de_para_rentabilidade():
        Json_Rentabilidade.carregar_json_interativo_rentabilidade_armazem()
        caminhos_rentabilidade = Json_Rentabilidade.carregar_json_interativo_rentabilidade_armazem()
        caminhos_rentabilidade = caminhos_rentabilidade["De_Para"]
        if not os.path.exists(caminhos_rentabilidade["path"]):
            with pd.ExcelWriter(caminhos_rentabilidade["path"], engine='xlsxwriter') as writer:
                for aba, dados in caminhos_rentabilidade.items():
                    if isinstance(dados, dict) and {'columns', 'data'}.issubset(dados):
                        df = pd.DataFrame(data=dados['data'], columns=dados['columns'])
                    else:
                        df = pd.DataFrame(dados)
                    
                    df.to_excel(writer, sheet_name=aba[:31], index=False) 

class De_Para:

    @classmethod
    def iniciar_de_para(cls):
        Json_Rentabilidade.gerar_arquivo_de_para_rentabilidade()
        caminhos = Json_Rentabilidade.carregar_json_interativo_rentabilidade_armazem()
        st.session_state["caminhos_arquivos"] = caminhos
        
    @classmethod
    def get_caminhos(cls):
        if "caminhos_arquivos" not in st.session_state:
            cls.iniciar_de_para()
        return st.session_state["caminhos_arquivos"]
    
    @classmethod
    def update_caminhos(cls):
        if "caminhos_arquivos" not in st.session_state:
            cls.iniciar_de_para()
        else:
            cls.iniciar_de_para()    
        return st.session_state["caminhos_arquivos"]

    @classmethod
    def arquivos_de_para(cls):
        caminhos = cls.get_caminhos()
        if "excel_file" not in st.session_state:
            excel_file = pd.ExcelFile(caminhos["De_Para"]["path"])
            nomes_abas = excel_file.sheet_names

            for aba in nomes_abas:
                if aba not in st.session_state:
                    df = pd.read_excel(caminhos["De_Para"]["path"], sheet_name=aba)
                    for col in df.columns:
                        df[col] = df[col].astype(str).str.strip()
                    df.columns = df.columns.str.strip()    
                    st.session_state[aba] = df

            st.session_state["excel_file"] = excel_file

        if "DRE_De_Para_Contas_Contabeis" in st.session_state:
            if "Grupo" in st.session_state["DRE_De_Para_Contas_Contabeis"].columns:
                st.session_state["DRE_De_Para_Contas_Contabeis"]["Grupo"] = (
                    st.session_state["DRE_De_Para_Contas_Contabeis"]["Grupo"]
                    .astype(str).str.strip().str.upper()
                )

class Relatorios_Rateio():
    @classmethod
    def carregar_volume(cls):

        if "concluidos" not in st.session_state: 
            st.session_state["concluidos"] = []
        for i in st.session_state["concluidos"]:
            st.write(i)

        if "nas_de_para" not in st.session_state: 
            st.session_state["nas_de_para"] = pd.DataFrame()


        caminhos = De_Para.get_caminhos()
            
        if "volumes" not in st.session_state:
            st.write("Volumes Base - Loading...")
            df = pd.read_excel(caminhos["Volumes_Base"]["path"])
            df.columns = df.columns.str.upper().str.strip()
            st.session_state["caminhos_arquivos_volumes"] = df
            st.session_state["volumes"] = df.copy()
        else:
            df = st.session_state["volumes"].copy()
        try:
            st.write(caminhos["volumes"]["novas_colunas"])
        except Exception as e:
            pass

        try:
            if caminhos["Volumes_Base"].get("novas_colunas"):
                for antiga, nova in caminhos["Volumes_Base"]["novas_colunas"].items():
                    if antiga in st.session_state["volumes"].columns:
                        st.session_state["volumes"].rename(columns={antiga: nova}, inplace=True)
        except Exception as e:
            st.warning(f"Erro ao renomear colunas: {e}")

        df = st.session_state["volumes"].copy()            

        new_dict = {}
        for col in caminhos["Volumes_Base"]["columns"]:
            if col not in df.columns:
                coluna_volume_nao_encontrada, coluna_volume_renomear = st.columns(2)
                with coluna_volume_nao_encontrada:
                    st.write(f"Coluna `{col}` não encontrada no arquivo.")
                with coluna_volume_renomear:
                    coluna_volume_renomear_var = st.selectbox(
                        f"Selecione a coluna para renomear como '{col}'", df.columns, key=col
                    )
                if st.button(f"Renomear para '{col}'", key=f"btn_{col}"):
                    df.rename(columns={coluna_volume_renomear_var: col}, inplace=True)
                    
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                        dados = json.load(file)
                    
                    # Garante que o dicionário 'novas_colunas' existe
                    if "novas_colunas" not in dados["volumes"]:
                        dados["volumes"]["novas_colunas"] = {}

                    # Atualiza ou adiciona o novo mapeamento
                    dados["volumes"]["novas_colunas"][coluna_volume_renomear_var] = col

                    # Salva o JSON atualizado
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                        json.dump(dados, file, indent=4)

                    st.success(f"Coluna '{coluna_volume_renomear_var}' renomeada para '{col}' e JSON atualizado.")

                        
                    caminhos=De_Para.update_caminhos() 
        if new_dict:
            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                dados = json.load(file)

            dados["Volumes_Base"]["novas_colunas"] = new_dict

            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                json.dump(dados, file, indent=4)
                st.write("JSON atualizado com sucesso.")
                st.rerun() 

        st.write("JSON atualizado com sucesso.")

        cols = [c.strip().upper() for c in caminhos["Volumes_Base"]["columns"]]
        df = df[cols]

        st.write("Volumes Base - Transformando Datas...")
        try:
            df["DATAFIMPEDIDO"] = pd.to_datetime(df["DATAFIMPEDIDO"], errors='coerce', dayfirst=True)
        except:
            df["DATAFIMPEDIDO"] = pd.to_datetime(
                df["DATAFIMPEDIDO"].astype(str).str.strip().str.split(" ").str[0],
                format="mixed", dayfirst=False
            )

        df["Tabela"] = "Relatório de Saída"
        df["Mês"] = df["DATAFIMPEDIDO"].dt.month.astype(str)
        df["Ano"] = df["DATAFIMPEDIDO"].dt.year.astype(str)

        tamanho = len(df[df["VOLUMES"] != 0])


        df = df[df["VOLUMES"] != 0].merge(
            st.session_state["Volumes_De_Para_Abreviacao"],
            how="left", left_on="CLIENTE", right_on="Area"
        ).drop(columns="Area")


        tamanho_2 = len(df)
        
        if tamanho != tamanho_2:
            st.session_state["tamanhos"].append(f"Verifique o Volumes_De_Para_Abreviacao para o adequacao, a tabela possui {tamanho} e após o de-para possui {tamanho_2} linhas, verifique se não possuem valores repetidos")     
            

        df = df.rename(columns={
            "SITE": "Filial UF", "CATEGORIAGRUPO": "Item",
            "VOLUMES": "saldo", "CLIENTE": "Area"
        })

        if df["Grupo"].isna().any():
            st.session_state.nas_depara = pd.concat([
                st.session_state.nas_depara, df[df["Grupo"].isna()]
            ])

        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)[["saldo"]].sum()
        df["Filial UF"] = df["Filial UF"].str.replace("ITJ", "SC")
        st.write("Volumes Base - Finalizado...")
        st.session_state["concluidos"].append("Volumes Base - Finalizado")
        st.session_state.caminhos_arquivos_volumes = df.copy()
        
        st.dataframe(df)
        del df
    
    @classmethod
    def adequacao(cls):
        caminhos = De_Para.get_caminhos()
        
        if "adequacao" not in st.session_state:
            st.write("Adequação Base - Carregando...")
            df = pd.read_excel(caminhos["Adequacao"]["path"])
            df.columns = df.columns.str.strip()
            st.session_state["caminhos_arquivos_adequacao"] = df
            st.session_state["adequacao"] = df.copy()
        else:
            df = st.session_state["adequacao"].copy()

        try:
            st.write(caminhos["Adequacao"]["novas_colunas"])
        except Exception as e:
            pass

        try:
            if caminhos["Adequacao"].get("novas_colunas"):
                for antiga, nova in caminhos["adequacao"]["novas_colunas"].items():
                    if antiga in st.session_state["adequacao"].columns:
                        st.session_state["adequacao"].rename(columns={antiga: nova}, inplace=True)
        except Exception as e:
            st.warning(f"Erro ao renomear colunas: {e}")

        df = st.session_state["adequacao"].copy()            

        new_dict = {}
        for col in caminhos["Adequacao"]["columns"]:
            if col not in df.columns:
                coluna_volume_nao_encontrada, coluna_volume_renomear = st.columns(2)
                with coluna_volume_nao_encontrada:
                    st.write(f"Coluna `{col}` não encontrada no arquivo.")
                with coluna_volume_renomear:
                    coluna_volume_renomear_var = st.selectbox(
                        f"Selecione a coluna para renomear como '{col}'", df.columns, key=col
                    )
                if st.button(f"Renomear para '{col}'", key=f"btn_{col}"):
                    df.rename(columns={coluna_volume_renomear_var: col}, inplace=True)
                    
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                        dados = json.load(file)
                    
                    # Garante que o dicionário 'novas_colunas' existe
                    if "novas_colunas" not in dados["Adequacao"]:
                        dados["Adequacao"]["novas_colunas"] = {}

                    # Atualiza ou adiciona o novo mapeamento
                    dados["Adequacao"]["novas_colunas"][coluna_volume_renomear_var] = col

                    # Salva o JSON atualizado
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                        json.dump(dados, file, indent=4)

                    st.success(f"Coluna '{coluna_volume_renomear_var}' renomeada para '{col}' e JSON atualizado.")

                    caminhos=De_Para.update_caminhos() 

        if new_dict:
            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                dados = json.load(file)

            dados["Adequacao"]["novas_colunas"] = new_dict

            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                json.dump(dados, file, indent=4)
                st.write("JSON atualizado com sucesso.")
                st.rerun() 

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

        tamanho = len(df)

        df = df.merge(st.session_state["Volumes_De_Para_Abreviacao"], how="left", left_on="Cliente", right_on="Area").drop(columns="Area")
        
        tamanho_2 = len(df)
        
        if tamanho != tamanho_2:
            st.session_state["tamanhos"].append(f"Verifique o Volumes_De_Para_Abreviacao para o adequacao, a tabela possui {tamanho} e após o de-para possui {tamanho_2} linhas, verifique se não possuem valores repetidos")        

        if df["Grupo"].isna().any():
            st.session_state.nas_depara = pd.concat([st.session_state.nas_depara, df[df["Grupo"].isna()]])

        df = df.rename(columns={"Serviço": "Area", "Nome Servico": "Item", "Qtde Real": "saldo", "Filial": "Filial UF"})
        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)[["saldo"]].sum()

        st.dataframe(df)

        st.write("Adequação Base - Finalizado...")
        st.session_state["concluidos"].append("Adequação Base - Finalizado")
        st.session_state.caminhos_arquivos_adequacao = df.copy()
        del df

    @classmethod
    def insumos(cls):
        caminhos = De_Para.get_caminhos()

        if "insumos" not in st.session_state:
            st.write("Insumos - Carregando...")
            df = pd.read_excel(caminhos["Insumos"]["path"])
            df.columns = df.columns.str.strip()
            st.session_state["caminhos_arquivos_insumos"] = df
            st.session_state["Insumos"] = df.copy()
        else:
            df = st.session_state["Insumos"].copy()

        try:
            st.write(caminhos["Insumos"]["novas_colunas"])
        except Exception as e:
            pass

        try:
            if caminhos["Insumos"].get("novas_colunas"):
                for antiga, nova in caminhos["Insumos"]["novas_colunas"].items():
                    if antiga in st.session_state["Insumos"].columns:
                        st.session_state["Insumos"].rename(columns={antiga: nova}, inplace=True)
        except Exception as e:
            st.warning(f"Erro ao renomear colunas: {e}")

        df = st.session_state["Insumos"].copy()            

        new_dict = {}
        for col in caminhos["Insumos"]["columns"]:
            if col not in df.columns:
                coluna_volume_nao_encontrada, coluna_volume_renomear = st.columns(2)
                with coluna_volume_nao_encontrada:
                    st.write(f"Coluna `{col}` não encontrada no arquivo.")
                with coluna_volume_renomear:
                    coluna_volume_renomear_var = st.selectbox(
                        f"Selecione a coluna para renomear como '{col}'", df.columns, key=col
                    )
                if st.button(f"Renomear para '{col}'", key=f"btn_{col}"):
                    df.rename(columns={coluna_volume_renomear_var: col}, inplace=True)
                    
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                        dados = json.load(file)
                    
                    # Garante que o dicionário 'novas_colunas' existe
                    if "novas_colunas" not in dados["Insumos"]:
                        dados["Insumos"]["novas_colunas"] = {}

                    # Atualiza ou adiciona o novo mapeamento
                    dados["Insumos"]["novas_colunas"][coluna_volume_renomear_var] = col

                    # Salva o JSON atualizado
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                        json.dump(dados, file, indent=4)

                    st.success(f"Coluna '{coluna_volume_renomear_var}' renomeada para '{col}' e JSON atualizado.")

                        
                    caminhos=De_Para.update_caminhos() 
        if new_dict:
            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                dados = json.load(file)

            dados["Insumos"]["novas_colunas"] = new_dict

            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                json.dump(dados, file, indent=4)
                st.rerun() 



        cols = caminhos["Insumos"]["columns"]
        df = df[cols].loc[df["Custo"] != 0]
        df["Tabela"] = "Insumos"
        df["ID"] = df["ID"].astype(str)
        df["Ano"] = df["ID"].str[:4]

        tamanho = len(df)
        df = df.merge(st.session_state["Embalagens_De_Para_Clientes"], how='left', left_on='NOMECLI', right_on='Nome Cliente')

        tamanho_2 = len(df)

        if tamanho != tamanho_2:
            st.session_state["tamanhos"].append(f"Verifique o Embalagens_De_Para_Clientes para o insumos, a tabela possui {tamanho} e após o de-para possui {tamanho_2} linhas, verifique se não possuem valores repetidos")        

        if df["Filial UF"].isna().any():
            st.session_state.nas_depara = pd.concat([st.session_state.nas_depara, df[df["Filial UF"].isna()]])

        tamanho = len(df)
        df = df.merge(st.session_state["Volumes_De_Para_Abreviacao"], how='left', left_on='Depositante', right_on='Area').drop(columns="Area")
        tamanho_2 = len(df)
        
        if tamanho != tamanho_2:
            st.session_state["tamanhos"].append(f"Verifique o Volumes_De_Para_Abreviacao para o insumos, a tabela possui {tamanho} e após o de-para possui {tamanho_2} linhas, verifique se não possuem valores repetidos")


        if df["Grupo"].isna().any():
            st.session_state.nas_depara = pd.concat([st.session_state.nas_depara, df[df["Grupo"].isna()]])

        df = df.rename(columns={"NOMECLI": "Area", "Insumo": "Item", "Custo": "saldo"})
        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)[["saldo"]].sum()
        df["saldo"] = df["saldo"] * 0.9075
        st.write("Adequação Insumos - Finalizado...")
        st.session_state["concluidos"].append("Adequação - Finalizado")
        st.session_state.caminhos_arquivos_insumos = df.copy()
        st.dataframe(df)
        del df

    @classmethod
    def faturamento(cls):
        caminhos = De_Para.get_caminhos()
        st.write("Faturamento - Carregando...")
        if "faturamento" not in st.session_state:
            df = pd.read_excel(
                caminhos["Faturamento"]["path"],
                sheet_name=caminhos["Faturamento"]["sheet_name"],
                header=caminhos["Faturamento"]["header"]
            )
            df.columns = df.columns.str.strip()            
            st.session_state["caminhos_arquivos_faturamento"] = df
            st.session_state["Faturamento"] = df.copy()
        else:
            df = st.session_state["Faturamento"].copy()

        try:
            st.write(caminhos["Faturamento"]["novas_colunas"])
        except Exception as e:
            pass

        try:
            if caminhos["Faturamento"].get("novas_colunas"):
                for antiga, nova in caminhos["Faturamento"]["novas_colunas"].items():
                    if antiga in st.session_state["Faturamento"].columns:
                        st.session_state["Faturamento"].rename(columns={antiga: nova}, inplace=True)
        except Exception as e:
            st.warning(f"Erro ao renomear colunas: {e}")

        df = st.session_state["Faturamento"].copy()            

        new_dict = {}
        for col in caminhos["Faturamento"]["columns"]:
            if col not in df.columns:
                coluna_volume_nao_encontrada, coluna_volume_renomear = st.columns(2)
                with coluna_volume_nao_encontrada:
                    st.write(f"Coluna `{col}` não encontrada no arquivo.")
                with coluna_volume_renomear:
                    coluna_volume_renomear_var = st.selectbox(
                        f"Selecione a coluna para renomear como '{col}'", df.columns, key=col
                    )
                if st.button(f"Renomear para '{col}'", key=f"btn_{col}"):
                    df.rename(columns={coluna_volume_renomear_var: col}, inplace=True)
                    
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                        dados = json.load(file)
                    
                    # Garante que o dicionário 'novas_colunas' existe
                    if "novas_colunas" not in dados["Faturamento"]:
                        dados["Faturamento"]["novas_colunas"] = {}

                    # Atualiza ou adiciona o novo mapeamento
                    dados["Faturamento"]["novas_colunas"][coluna_volume_renomear_var] = col

                    # Salva o JSON atualizado
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                        json.dump(dados, file, indent=4)

                    st.success(f"Coluna '{coluna_volume_renomear_var}' renomeada para '{col}' e JSON atualizado.")

                        
                    caminhos=De_Para.update_caminhos() 
        if new_dict:
            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                dados = json.load(file)

            dados["Faturamento"]["novas_colunas"] = new_dict

            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                json.dump(dados, file, indent=4)
                st.rerun() 

        st.write("JSON atualizado com sucesso.")

        df = df[df["EMPRESA"].isin(caminhos["Faturamento"]["empresa"]) &
                df["ANO"].isin(caminhos["Faturamento"]["ano"]) &
                df["VERSÃO"].isin(caminhos["Faturamento"]["versao"]) &
                df["RECEITA"].isin(caminhos["Faturamento"]["receita"]) &
                df["VALOR R$"].notna()]
        st.dataframe(df)
        
        df = df.groupby(["ANO", "MÊS", "EMPRESA", "FILIAL", "CLIENTE", "TIPO"], as_index=False)[["VALOR R$"]].sum()
        df["VALOR R$"] = df["VALOR R$"] * 0.9075

        df["ANO"] = df["ANO"].astype(str).str.split()
        df["ANO"] = df["ANO"].apply(lambda x: x[0] if isinstance(x, list) else x)
        st.dataframe(df)

        tamanho = len(df)
        st.dataframe(df)
        df = df.merge(st.session_state["DRE_De_Para_Filial"], how='left', left_on="FILIAL", right_on="Filial")
        
        tamanho_2 = len(df)
        if tamanho != tamanho_2:
            st.session_state["tamanhos"].append(f"Verifique o De_Para_Filial para o faturamento, a tabela possui {tamanho} e após o de-para possui {tamanho_2} linhas, verifique se não possuem valores repetidos")

        if df["Filial"].isna().any():
            st.session_state.nas_depara = pd.concat([st.session_state.nas_depara, df[df["Filial"].isna()]])

        df["Tabela"] = "Faturamento"
        df["Item"] = "Faturamento"

        df = df.rename(columns={
            "ANO": "Ano", "MÊS": "Mês", "CLIENTE": "Grupo",
            "TIPO": "Area", "VALOR R$": "saldo"
        })

        df = df[["Tabela", "Ano", "Mês", "Filial UF", "Grupo", "Area", "Item", "saldo"]]
        st.write("Faturamento - Finalizado...")
        st.session_state["concluidos"].append("Faturamento - Finalizado")
        st.session_state.caminhos_arquivos_faturamento = df.copy()
        st.dataframe(df)
        del df

    @classmethod
    def ocupacao_armazem(cls):
        caminhos = De_Para.get_caminhos()
        abas = caminhos["Ocupacao_Armazem"]["sheet_name"]
        if "Ocupacao_Armazem" not in st.session_state:
            st.session_state["Ocupacao_Armazem"] = pd.DataFrame()

        if "caminhos_arquivos_ocupacao" not in st.session_state:
            st.session_state["caminhos_arquivos_ocupacao"] = pd.DataFrame()
        else:
            st.session_state["caminhos_arquivos_ocupacao"] = st.session_state["Ocupacao_Armazem"].copy()

        st.write("ocupacao_armazem - Carregando...")      
        for l in abas:
            try:
                excel = pd.ExcelFile(caminhos["Ocupacao_Armazem"]["path"])
                pd.read_excel(excel, sheet_name=l)
            
            except ValueError as e:
                if "Worksheet" in str(e) or "not found" in str(e):
                    st.warning(f"Aba '{l}' não encontrada no arquivo: {caminhos["Ocupacao_Armazem"]["path"]}")
                    abas_disponiveis = excel.sheet_names
                    st.write(abas_disponiveis)
                    nova_aba = st.selectbox(f"Selecione a nova aba para '{l}':", abas_disponiveis, key=f"sheet_{l}")

                    if st.button("Atualizar aba e carregar novamente", key=f"botao_{l}"):
                        try:
                            index = caminhos["Ocupacao_Armazem"]["sheet_name"].index(l)
                            caminhos["Ocupacao_Armazem"]["sheet_name"][index] = nova_aba
                        except ValueError:
                            st.error(f"Aba '{l}' não encontrada na lista.")
                        caminhos["Ocupacao_Armazem"]["sheet_name"] = [s.replace(l, nova_aba) for s in caminhos["Ocupacao_Armazem"]["sheet_name"]]
                        with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as f:
                            json.dump(caminhos, f, indent=4)
                        st.success(f"Aba atualizada para '{nova_aba}'. Recarregando...")
                        st.rerun()

                    st.stop()
                else:
                    raise e

        for i in abas:
            if f"Ocupacao_Armazem_{i}" not in st.session_state:
                Ocupacao_Armazem = pd.read_excel(caminhos["Ocupacao_Armazem"]["path"], sheet_name=i, header=caminhos["Ocupacao_Armazem"]["header"])
                st.session_state[f"Ocupacao_Armazem_{i}"] = Ocupacao_Armazem
            else:
                Ocupacao_Armazem = st.session_state[f"Ocupacao_Armazem_{i}"].copy()

            month = caminhos["Ocupacao_Armazem"]["escrita_mes"]
            
            colunas_para_remover = []

            Posicao_Palet_SP_Alterado = st.session_state[f"Ocupacao_Armazem_{i}"].copy()
            Posicao_Palet_SP_Alterado = Posicao_Palet_SP_Alterado.fillna(0)
            for nome1,nome2 in Posicao_Palet_SP_Alterado.columns.values:
                try:
                    if Posicao_Palet_SP_Alterado[(nome1, nome2)].sum() == 0:
                        colunas_para_remover.append((nome1,nome2))
                    else:
                        pass
                except:
                    pass
            Posicao_Palet_SP_Alterado = Posicao_Palet_SP_Alterado.drop(columns=colunas_para_remover)

            Clientes_2 = pd.DataFrame()
            for column in Posicao_Palet_SP_Alterado.columns:
                if isinstance(column, tuple):
                    novo_nome = column[0].replace("  ", " ").strip()
                    Clientes = Posicao_Palet_SP_Alterado.rename(columns={column: novo_nome})
                else:
                    novo_nome = column.replace("  ", " ").strip()
                    Clientes = Posicao_Palet_SP_Alterado.rename(columns={column: novo_nome})
                Clientes = pd.DataFrame([novo_nome], columns=["Clientes"])
                Clientes_2 = pd.concat([Clientes_2, Clientes], ignore_index=True)
            
            
            colunas_mes = [col for col in Posicao_Palet_SP_Alterado.columns if month in str(col[0]) or month in str(col[1])]
            Clientes_2 = Clientes_2.loc[(Clientes_2["Clientes"]!=colunas_mes[0][0])]

            Colunas = Posicao_Palet_SP_Alterado.columns
            Registros_Totais = pd.DataFrame()
            Posicao_Pallet = pd.DataFrame()

            for outro in Colunas:
                if len(Posicao_Palet_SP_Alterado[outro[0]].columns.values) >1:
                    Registros_Totais[outro] = Posicao_Palet_SP_Alterado[outro]
                else:
                    Posicao_Pallet[outro] = Posicao_Palet_SP_Alterado[outro]

            if not Posicao_Pallet.empty:
                print("iguais")

                renomear = []
                for column in Posicao_Pallet.columns.values:
                    if isinstance(column, tuple):
                        name1, name2 = column
                        if name2 == "Mês":
                            renomear.append(name2)
                        else:
                            renomear.append(name1)

                Posicao_Pallet.columns = renomear

                    #Inserindo a Filial
                Posicao_Pallet = (Posicao_Pallet.set_index(["Mês"])
                                        .stack()
                                        .reset_index(name='Ocupação')
                                        .rename(columns={'level_1':'Cliente'}))

                Posicao_Pallet["Filial"] = i

            if not Registros_Totais.empty:
                print("Não iguais")

                #Padronizando o nome dos clientes, retirando o tipo de armazenagem da header(Ambiente, Camara Fria, etc), mantendo apenas o que possui total
                Registros_Totais.columns = Registros_Totais.columns.astype(str).str.upper()
                Clientes_Sem_Total =  Registros_Totais.loc[:, ~Registros_Totais.columns.str.contains("TOTAL", case=False, na=False)]
                if not Clientes_Sem_Total.empty:
                    Clientes_Sem_Total = Clientes_Sem_Total.groupby(level=0, axis=1).sum()
                
                #Fazendo substituições para voltar a ser Tupla
                Registros_Totais = Registros_Totais.loc[:, Registros_Totais.columns.str.contains("TOTAL", case=False, na=False)]
                Registros_Totais.columns = Registros_Totais.columns.str.replace("(","")
                Registros_Totais.columns = Registros_Totais.columns.str.replace(")","")
                Registros_Totais.columns = Registros_Totais.columns.str.replace("'","") 
                Registros_Totais.columns = [tuple(col.split(',')) for col in Registros_Totais.columns]
                
                #Renomeando as colunas, retirando o multiindex
                renomear = []
                for column in Registros_Totais.columns.values:
                    if isinstance(column, tuple):
                        name1, name2 = column
                        renomear.append(name1)
                    else:
                        substituir = "[(')"
                        column =column.replace(substituir,"")
                        renomear.append(str(column).split(',')[0])
                Registros_Totais.columns = renomear

                if not Clientes_Sem_Total.empty:
                #Por garantia, foi feito esse bloco para caso ocorra de algum cliente ter mais de uma coluna e não ter Total
                    clientes_para_remover = []

                    for i in Registros_Totais.columns.values:
                        Clientes_Sem_Total = Clientes_Sem_Total.loc[:, ~Clientes_Sem_Total.columns.str.contains(i, case=False, na=False)]


                    Clientes_Sem_Total.columns = Clientes_Sem_Total.columns.str.replace("(","")
                    Clientes_Sem_Total.columns = Clientes_Sem_Total.columns.str.replace(")","")
                    Clientes_Sem_Total.columns = Clientes_Sem_Total.columns.str.replace("'","") 
                    Clientes_Sem_Total.columns = [tuple(col.split(',')) for col in Clientes_Sem_Total.columns]
                    
                    #Renomeando as colunas, retirando o multiindex
                    renomear = []
                    for column in Clientes_Sem_Total.columns.values:
                        if isinstance(column, tuple):
                            name1, name2 = column
                            renomear.append(name1)
                        else:
                            substituir = "[(')"
                            column =column.replace(substituir,"")
                            renomear.append(str(column).split(',')[0])
                    Clientes_Sem_Total.columns = renomear
                    Clientes_Sem_Total = Clientes_Sem_Total.groupby(level=0, axis=1).sum()
                    Registros_Totais = pd.concat([Registros_Totais,Clientes_Sem_Total],axis=1)


                #Inserindo o Mês
                Registros_Totais["Mês"] = list(Posicao_Pallet["Mês"].unique())
                
                #Transformando as Colunas em linhas
                Registros_Totais = (Registros_Totais.set_index(["Mês"])
                                        .stack()
                                        .reset_index(name='Ocupação')
                                        .rename(columns={'level_1':'Cliente'}))

                #Inserindo a Filial
                Registros_Totais["Filial"] = i

            #Validações para criar o DataFrame final baseado se eles estão vazios ou não
            if not Posicao_Pallet.empty and not Registros_Totais.empty:
                Posicao_Palet_SP_Alterado = pd.concat([Posicao_Pallet,Registros_Totais])
            elif Posicao_Pallet.empty and not Registros_Totais.empty:
                Posicao_Palet_SP_Alterado = Registros_Totais.copy()
            elif not Posicao_Pallet.empty and Registros_Totais.empty:
                Posicao_Palet_SP_Alterado = Posicao_Pallet.copy()

            #Retirando possiveis valores vazios dos meses por ter linhas adicionais nos arquivos
            Posicao_Palet_SP_Alterado.dropna(subset="Mês",inplace=True)
            Posicao_Palet_SP_Alterado["Cliente"] = Posicao_Palet_SP_Alterado["Cliente"].str.strip()
            Posicao_Palet_SP_Alterado["Cliente"] = [re.sub(r"\s+", " ", cliente).strip() for cliente in Posicao_Palet_SP_Alterado["Cliente"]]

            #Verificando se todos os clientes estão presentes
            Clientes_2 =pd.DataFrame(Clientes_2["Clientes"].copy())
            Consolidar_SP = Clientes_2[~Clientes_2["Clientes"].isin(Posicao_Palet_SP_Alterado["Cliente"].unique())]

            if Consolidar_SP.empty:
                st.write("Ok")
            else:
                pass

            filtro = ~Posicao_Palet_SP_Alterado["Cliente"].str.contains("level", case=False, na=False, regex=True)

            Consolidar_SP = Posicao_Palet_SP_Alterado.loc[
                (Posicao_Palet_SP_Alterado["Mês"] != 0) & filtro
            ].copy()

            
            Consolidar_SP["Mês"] = pd.to_datetime(Consolidar_SP["Mês"], format="mixed",errors="coerce")
            Consolidar_SP["Ano"] = Consolidar_SP["Mês"].dt.year
            Consolidar_SP["Mês"] = Consolidar_SP["Mês"].dt.month

            st.session_state["caminhos_arquivos_ocupacao"] = pd.concat([st.session_state["caminhos_arquivos_ocupacao"],Consolidar_SP],ignore_index=True)
            st.session_state["Ocupacao_Armazem"] = pd.concat([st.session_state["caminhos_arquivos_ocupacao"],Consolidar_SP],ignore_index=True)
            st.dataframe(st.session_state["caminhos_arquivos_ocupacao"])
                
        df = st.session_state["caminhos_arquivos_ocupacao"].copy()

        try:
            st.write(caminhos["Ocupacao_Armazem"]["novas_colunas"])
        except Exception as e:
            pass

        try:
            if caminhos["Ocupacao_Armazem"].get("novas_colunas"):
                for antiga, nova in caminhos["Ocupacao_Armazem"]["novas_colunas"].items():
                    if antiga in st.session_state["caminhos_arquivos_ocupacao"].columns:
                        st.session_state["caminhos_arquivos_ocupacao"].rename(columns={antiga: nova}, inplace=True)
        except Exception as e:
            st.warning(f"Erro ao renomear colunas: {e}")

        df = st.session_state["caminhos_arquivos_ocupacao"].copy()            

        new_dict = {}
        for col in caminhos["Ocupacao_Armazem"]["columns"]:
            if col not in df.columns:
                coluna_volume_nao_encontrada, coluna_volume_renomear = st.columns(2)
                with coluna_volume_nao_encontrada:
                    st.write(f"Coluna `{col}` não encontrada no arquivo.")
                with coluna_volume_renomear:
                    coluna_volume_renomear_var = st.selectbox(
                        f"Selecione a coluna para renomear como '{col}'", df.columns, key=col
                    )
                if st.button(f"Renomear para '{col}'", key=f"btn_{col}"):
                    df.rename(columns={coluna_volume_renomear_var: col}, inplace=True)
                    
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                        dados = json.load(file)
                    
                    # Garante que o dicionário 'novas_colunas' existe
                    if "novas_colunas" not in dados["Ocupacao_Armazem"]:
                        dados["Ocupacao_Armazem"]["novas_colunas"] = {}

                    # Atualiza ou adiciona o novo mapeamento
                    dados["Ocupacao_Armazem"]["novas_colunas"][coluna_volume_renomear_var] = col

                    # Salva o JSON atualizado
                    with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                        json.dump(dados, file, indent=4)

                    st.success(f"Coluna '{coluna_volume_renomear_var}' renomeada para '{col}' e JSON atualizado.")

                        
                    caminhos=De_Para.update_caminhos() 
        if new_dict:
            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "r") as file:
                dados = json.load(file)

            dados["Ocupacao_Armazem"]["novas_colunas"] = new_dict

            with open(Json.caminho_json_rentabilidade_armazem_arquivos, "w") as file:
                json.dump(dados, file, indent=4)
                st.rerun() 

        tamanho = len(df)
        
        df = df.merge(st.session_state["De_Para_Grupos_Ocupacao"],how="left",left_on=["Cliente","Filial"],right_on=["Cliente","Filial"])

        tamanho_2 = len(df)
        if tamanho != tamanho_2:
            st.session_state["tamanhos"].append(f"Verifique o De_Para_Grupos_Ocupacao para o faturamento, a tabela possui {tamanho} e após o de-para possui {tamanho_2} linhas, verifique se não possuem valores repetidos")

        if df["Grupo"].isna().any():
            st.session_state.nas_depara = pd.concat([st.session_state.nas_depara, df[df["Grupo"].isna()]])

        df["Tabela"] = "Ocupação Armazenagem"    
        df = df[["Tabela","Ano","Mês","Filial","Area",
                                        "Grupo","Item","Ocupação"]].loc[df["Ocupação"] != 0]

        df = df.rename(columns={"Ocupação":"saldo","Filial":"Filial UF"})
        st.session_state["caminhos_arquivos_ocupacao"] = df.copy()
        st.write("ocupacao_armazem - Finalizado...")     

class Relatorios_DRE():
    @classmethod
    def tratar_razao(cls):
        if "caminhos_arquivos" not in st.session_state:
            De_Para.arquivos_de_para()
        caminhos = st.session_state["caminhos_arquivos"]

        if "Razao_Farma_Consolidado_Sem_Tratamento" not in st.session_state:
            Razao_Farma = pd.DataFrame()
            
            for i in caminhos["DRE"]["sheet_name"]:
                st.write(i)
                Razao = pd.read_excel(caminhos["DRE"]["path"], sheet_name=i, header=caminhos["DRE"]["header"])
                Razao_Farma = pd.concat([Razao_Farma, Razao], ignore_index=True)
                st.session_state["Razao_Farma_Consolidado_Sem_Tratamento"] = Razao_Farma.copy()
        else:
            Razao_Farma = st.session_state["Razao_Farma_Consolidado_Sem_Tratamento"].copy()

        Razao_Farma.columns = Razao_Farma.columns.astype(str).str.strip()

        Razao_Farma = Razao_Farma.drop(columns="Mês")
    
        colunas_utilizar = [coluna for coluna in caminhos["DRE"]["colunas_str"] if coluna in Razao_Farma.columns]
        for coluna in colunas_utilizar:
            Razao_Farma[coluna] = Razao_Farma[coluna].astype(str).str.strip()

        Tamanho_Original = len(Razao_Farma)
        Razao_Farma_Consolidado = Razao_Farma.copy()
        
        if not pd.api.types.is_datetime64_any_dtype(Razao_Farma_Consolidado["Data"]):
            try:
                Razao_Farma_Consolidado["Data"] = pd.to_datetime(Razao_Farma_Consolidado["Data"], errors="coerce")
            except Exception as e:
                st.error(f"Erro ao converter coluna 'Data' para datetime: {e}")

        Razao_Farma_Consolidado["Ano"] = Razao_Farma_Consolidado["Data"].dt.year.astype(str)
        Razao_Farma_Consolidado["Mês"] = Razao_Farma_Consolidado["Data"].dt.month.astype(str)

        if "Grupo" in Razao_Farma_Consolidado.columns:
            Razao_Farma_Consolidado = Razao_Farma_Consolidado.drop(columns=["Grupo"])


            
        Razao_Farma_Consolidado = Razao_Farma_Consolidado.merge(
            st.session_state["DRE_De_Para_Centro_Custo"], how="left", on="Centro de Custo"
        )
        if len(Razao_Farma_Consolidado) != Tamanho_Original:
            st.write("De-Para do Centro de Custo com Divergência - Verifique o arquivo a aba DRE_De_Para_Centro_Custo")
            st.session_state["tamanhos"].append(f"Verifique o DRE_De_Para_Centro_Custo para o Razão, a tabela possui {Razao_Farma_Consolidado} e após o de-para possui {Tamanho_Original} linhas, verifique se não possuem valores repetidos")  

        if Razao_Farma_Consolidado["Centro de Custo"].isna().any():
            st.session_state.nas_depara_razao = pd.concat([st.session_state.nas_depara_razao, Razao_Farma_Consolidado[Razao_Farma_Consolidado["Centro Custo"].isna()]])


        Razao_Farma_Consolidado = Razao_Farma_Consolidado.merge(
            st.session_state["DRE_De_Para_Item_Conta"], how="left", on="Item"
        )
        if len(Razao_Farma_Consolidado) != Tamanho_Original:
            st.write("De-Para Item Conta com Divergência - Verifique o arquivo a aba DRE_De_Para_Item_Conta")
            st.session_state["tamanhos"].append(f"Verifique o DRE_De_Para_Item_Conta para o Razão, a tabela possui {Razao_Farma_Consolidado} e após o de-para possui {Tamanho_Original} linhas, verifique se não possuem valores repetidos")  


        if Razao_Farma_Consolidado["Nome"].isna().any():
            st.session_state.nas_depara_razao = pd.concat([st.session_state.nas_depara_razao, Razao_Farma_Consolidado[Razao_Farma_Consolidado["Nome"].isna()]])
            
        Razao_Farma_Consolidado = Razao_Farma_Consolidado.merge(
            st.session_state["DRE_De_Para_Filial"], how="left", on="Filial"
        )
        if len(Razao_Farma_Consolidado) != Tamanho_Original:
            st.write("De-Para Filial com Divergência - Verifique o arquivo a aba DRE_De_Para_Filial")
            st.session_state["tamanhos"].append(f"Verifique o DRE_De_Para_Filial para o Razão, a tabela possui {Razao_Farma_Consolidado} e após o de-para possui {Tamanho_Original} linhas, verifique se não possuem valores repetidos")

        if Razao_Farma_Consolidado["Filial UF"].isna().any():
            st.session_state.nas_depara_razao = pd.concat([st.session_state.nas_depara_razao, Razao_Farma_Consolidado[Razao_Farma_Consolidado["Filial UF"].isna()]])  

        Razao_Farma_Consolidado["Concat Razão"] = (
            Razao_Farma_Consolidado["Conta"] + Razao_Farma_Consolidado["Tipo CC"]
        ).astype(str).str.strip()


        Razao_Farma_Consolidado = Razao_Farma_Consolidado.merge(
            st.session_state["DRE_De_Para_Contas_Contabeis"], how='left', on='Concat Razão'
        )
        if len(Razao_Farma_Consolidado) != Tamanho_Original:
            st.write("De-Para Contas Contabeis com Divergência - Verifique o arquivo a aba DRE_De_Para_Contas_Contabeis")
            st.session_state["tamanhos"].append(f"Verifique o DRE_De_Para_Contas_Contabeis para o Razão, a tabela possui {Razao_Farma_Consolidado} e após o de-para possui {Tamanho_Original} linhas, verifique se não possuem valores repetidos")  
        

        fallback_merge = st.session_state["DRE_De_Para_Contas_Contabeis"].loc[
            st.session_state["DRE_De_Para_Contas_Contabeis"]["Concat Razão"] == "nan"
        ]

        sem_grupo = Razao_Farma_Consolidado["Grupo Financeiro"].isna()

        Razao_Farma_Consolidado = pd.concat([
            Razao_Farma_Consolidado[~sem_grupo],
            Razao_Farma_Consolidado[sem_grupo].drop(
                columns=["conta", "descrição completa", "descrição resumida", "Grupo", "Grupo Financeiro"], errors="ignore"
            ).merge(fallback_merge, how="left", left_on="Conta", right_on="conta")
        ])

        Razao_Farma_Consolidado["Concat Razão"] = Razao_Farma_Consolidado["Concat Razão"].fillna(
            Razao_Farma_Consolidado.get("Concat Razão_x", "")
        )
        Razao_Farma_Consolidado = Razao_Farma_Consolidado.drop(
            columns=[col for col in ["Concat Razão_x", "Concat Razão_y"] if col in Razao_Farma_Consolidado.columns], errors="ignore"
        )

        st.session_state["tamanhos"].append(f"Verifique o DRE_De_Para_Contas_Contabeis para os valores vazios usando como chave apenas o número da conta contabil Razão, a tabela possui {Razao_Farma_Consolidado} e após o de-para possui {Tamanho_Original} linhas, verifique se não possuem valores repetidos")  


        if Razao_Farma_Consolidado["Grupo"].isna().any():
            st.session_state.nas_depara_razao = pd.concat([st.session_state.nas_depara_razao, Razao_Farma_Consolidado[Razao_Farma_Consolidado["Grupo"].isna()]])  


        Itens_Conta_Desconsiderar = st.session_state["Item_De_Para_Filial_Depreciacao"].loc[
            st.session_state["Item_De_Para_Filial_Depreciacao"]["Filial UF"] == "DESC", "Item"
        ].unique()

        Razao_Farma_Consolidado = Razao_Farma_Consolidado.loc[
            ~Razao_Farma_Consolidado["Item"].isin(Itens_Conta_Desconsiderar)
        ]

        st.session_state["Razao_Farma_Consolidado"] = Razao_Farma_Consolidado.copy()
        st.session_state["Razao_Farma_Consolidado_Para_Download"] = Razao_Farma_Consolidado.copy()
        st.session_state["concluidos"].append("Faturamento - Finalizado")
        
        # ============================================================
        # DEBUG: Exportar Razão tratada (OLD) para comparar com V2
        # ============================================================
        try:
            debug_path = r"C:\Projetos\DRE\Relatórios\Razao_Tratada_OLD.xlsx"
            os.makedirs(os.path.dirname(debug_path), exist_ok=True)

            # Se quiser salvar “linha a linha”
            Razao_Farma_Consolidado.to_excel(debug_path, index=False)

            st.write(f"[DEBUG] Razao_Tratada_OLD exportada para: {debug_path}")
        except Exception as e:
            st.warning(f"Erro ao exportar Razao_Tratada_OLD: {e}")
        
    @classmethod
    def Embalagem_Adequa(cls):
        if "Razao_Farma_Consolidado" not in st.session_state:
            st.error("Iniciando o tratamento da Razao_Farma_Consolidado")
            cls.tratar_razao()

        if "Razao_Farma_Consolidado_Sem_Tratamento" in st.session_state:
            st.session_state["Razao_Farma_Consolidado"] = st.session_state["Razao_Farma_Consolidado_Para_Download"].copy()      

        @staticmethod
        def adequacao():
            df = st.session_state["Razao_Farma_Consolidado"].copy()
            df = df.loc[(df["Item"] == "10110") & (df["Grupo"] == "PESSOAL OPER")]
            df["Tabela"] = "Folha Adequação"
            df = df.rename(columns={"Título Conta": "Area"})
        
            df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
            mascara_remover = (st.session_state["Razao_Farma_Consolidado"]["Item"] == "10110") & (st.session_state["Razao_Farma_Consolidado"]["Grupo"] == "PESSOAL OPER")

            st.session_state["Razao_Farma_Consolidado"] = st.session_state["Razao_Farma_Consolidado"][~mascara_remover]
            
            return df
        @staticmethod
        def embalagens():
            df = st.session_state["Razao_Farma_Consolidado"].copy()
            df=df.loc[df["Título Conta"] == "MATERIAL DE EMBALAGEM"].drop(columns={"Grupo"})
            df["Tabela"] = "MATERIAL DE EMBALAGEM"
            df["Area"] = "Desconhecido"
            df = df.rename(columns={"Sigla": "Grupo"})
            df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
            
            mascara_remover = st.session_state["Razao_Farma_Consolidado"]["Título Conta"].isin(["MATERIAL DE EMBALAGEM"])
            st.session_state["Razao_Farma_Consolidado"] = st.session_state["Razao_Farma_Consolidado"][~mascara_remover]

            return df
        @staticmethod
        def custos_financeiros():
            df = st.session_state["Razao_Farma_Consolidado"].loc[
                st.session_state["Razao_Farma_Consolidado"]["Grupo Financeiro"].isin(["DEPREC/AMORT", "CUSTOS FINANCEIROS"])
            ].copy()
            df = df.drop(columns={"Grupo"})
            df = df.rename(columns={"Título Conta": "Area", "Grupo Financeiro": "Grupo"})
            df["Tabela"] = "Custos Financeiros"

            depre = df[df["Grupo"] == "DEPREC/AMORT"].copy().drop(columns=["Filial UF"])
            depre["Item"] = depre["Item"].astype(str).str.strip()
            depre = depre.merge(st.session_state["Item_De_Para_Filial_Depreciacao"], how='left', on='Item')
            depre = depre.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Grupo", "Area", "Item"], as_index=False)["saldo"].sum()

            outros = df[df["Grupo"] != "DEPREC/AMORT"].copy()
            outros = outros.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Grupo", "Area", "Item"], as_index=False)["saldo"].sum()

            st.session_state["Razao_Farma_Consolidado"] = st.session_state["Razao_Farma_Consolidado"].loc[
                ~st.session_state["Razao_Farma_Consolidado"]["Grupo Financeiro"].isin(["DEPREC/AMORT", "CUSTOS FINANCEIROS"])]

            return pd.concat([outros, depre], ignore_index=True)
        @staticmethod
        def iss_():
            df = st.session_state["Razao_Farma_Consolidado"].loc[
                st.session_state["Razao_Farma_Consolidado"]["Grupo"] == "ISS"
            ].copy()

            mapa_filial = {"10802": "GO", "10302": "SP", "10702": "RJ", "11002": "SC"}
            df["Filial UF"] = df["Item"].map(mapa_filial).fillna(df["Filial UF"])
            df["Tabela"] = "ISS"
            df["Area"] = "ISS"
            df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

            st.session_state["Razao_Farma_Consolidado"] = st.session_state["Razao_Farma_Consolidado"].loc[
                ~st.session_state["Razao_Farma_Consolidado"]["Grupo"].isin(["ISS"])]
            return df
        @staticmethod
        def outros_impostos():
            df = st.session_state["Razao_Farma_Consolidado"].loc[
                st.session_state["Razao_Farma_Consolidado"]["Grupo"].isin(["PIS", "COFINS", "ICMS"])
            ].copy()
            df["Tabela"] = "Outros Impostos"
            df["Area"] = "Outros Impostos"
            df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

            st.session_state["Razao_Farma_Consolidado"] = st.session_state["Razao_Farma_Consolidado"].loc[
                ~st.session_state["Razao_Farma_Consolidado"]["Grupo"].isin(["PIS", "COFINS", "ICMS", "ISS"])]
            return df
        @staticmethod
        def taxas_custas():
            df = st.session_state["Razao_Farma_Consolidado"].loc[
                st.session_state["Razao_Farma_Consolidado"]["Grupo"] == "IMPOSTOS OPER"
            ].copy()
            df["Tabela"] = "Custos Operacionais Indiretos - Taxas"

            df.loc[(df["Centro Custo"] == "Operação Armazenagem") & (df["Sigla"] != "Desconhecido"), "Tabela"] = "Custos Operacionais - Taxas"
            df.loc[(df["Centro Custo"] != "Operação Armazenagem") & (df["Sigla"] != "Desconhecido"), "Tabela"] = "Custos Operacionais Outros - Taxas"

            df = df.drop(columns={"Grupo"})
            df = df.rename(columns={"Sigla": "Grupo", "Título Conta": "Area"})
            df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

            st.session_state["Razao_Farma_Consolidado"] = st.session_state["Razao_Farma_Consolidado"].loc[
                ~st.session_state["Razao_Farma_Consolidado"]["Grupo"].isin(["IMPOSTOS OPER"])]
            return df

        # Execução dos blocos internos
        df_adequacao = adequacao()
        df_embalagens = embalagens()
        df_custos_fin = custos_financeiros()
        df_iss = iss_()
        df_outros = outros_impostos()
        df_taxas = taxas_custas()
        st.session_state["concluidos"].append("Razão - Adequação, Embalagens, Custos Financeiros, ISS, Outros Impostos, Taxas - Adicionados")
        return pd.concat([df_adequacao, df_embalagens, df_custos_fin, df_iss, df_outros, df_taxas], ignore_index=True)

    @classmethod
    def Overhead(cls):
        if "Razao_Farma_Consolidado" not in st.session_state:
            cls.tratar_razao()
            cls.Embalagem_Adequa()

        @staticmethod
        def overhead_nao_operacional():
            df = st.session_state["Razao_Farma_Consolidado"].loc[
                st.session_state["Razao_Farma_Consolidado"]["Tipo CC"] != "Oper"
            ].copy()

            df = df.rename(columns={"Grupo": "Area"})
            df[["Tabela", "Grupo", "Item"]] = "Overhead"
            df = df[df["Area"] != "SERVIÇOS"]
            df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

            st.session_state["Razao_Farma_Consolidado"] = st.session_state["Razao_Farma_Consolidado"].loc[
                st.session_state["Razao_Farma_Consolidado"]["Tipo CC"] == "Oper"
            ]
            return df
        @staticmethod
        def indenizacao_trabalhista():
            df = st.session_state["Razao_Farma_Consolidado"].loc[
                (st.session_state["Razao_Farma_Consolidado"]["Conta"] == "60301020108") &
                (st.session_state["Razao_Farma_Consolidado"]["Tipo CC"] == "Oper")
            ].copy()

            df = df.rename(columns={"Grupo": "Area"})
            df[["Tabela", "Grupo", "Item"]] = "Overhead"
            df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()

            st.session_state["Razao_Farma_Consolidado"] = st.session_state["Razao_Farma_Consolidado"].loc[
                st.session_state["Razao_Farma_Consolidado"]["Conta"] != "60301020108"
            ]
            return df
        
        df_overhead = overhead_nao_operacional()
        df_indenizacao = indenizacao_trabalhista()
        st.session_state["concluidos"].append("Razão - Overhead - Adicionados")

        return pd.concat([df_overhead, df_indenizacao], ignore_index=True)

    @classmethod
    def farma_direto_indireto(cls):
        if "Razao_Farma_Consolidado" not in st.session_state:
            cls.tratar_razao()
            cls.Embalagem_Adequa()
            cls.Overhead()

        df = st.session_state["Razao_Farma_Consolidado"]

        df["Tabela_Consolidada"] = "Tabela_Consolidada"
        df["Tabela_Consolidada"] = "Tabela"

        cond_direto = df["Sigla"] != "Desconhecido"
        cond_indireto = df["Sigla"] == "Desconhecido"
        cond_armazem = df["Centro Custo"] == "Operação Armazenagem"
        cond_outros = df["Centro Custo"] != "Operação Armazenagem"

        # Farma Direto
        st.session_state["Razao_Farma_Consolidado"].loc[cond_direto & cond_armazem, "Tabela_Consolidada"] = "Farma Direto"
        st.session_state["Razao_Farma_Consolidado"].loc[cond_direto & cond_outros, "Tabela_Consolidada"] = "Farma Direto"

        # Farma Indireto
        st.session_state["Razao_Farma_Consolidado"].loc[cond_indireto & cond_armazem, "Tabela_Consolidada"] = "Farma Indireto"
        st.session_state["Razao_Farma_Consolidado"].loc[cond_indireto & cond_outros, "Tabela_Consolidada"] = "Farma Indireto"

    @classmethod
    def custos_alocados(cls):
        if "Razao_Farma_Consolidado" not in st.session_state:
            cls.tratar_razao()
            cls.Embalagem_Adequa()
            cls.Overhead()
            cls.farma_direto_indireto()

        df = st.session_state["Razao_Farma_Consolidado"].copy()

        # Folha de Pagamento
        cond_farma_direto = df["Tabela_Consolidada"] == "Farma Direto"
        cond_farma_indireto = df["Tabela_Consolidada"] == "Farma Indireto"
        cond_pessoal_oper = df["Grupo"] == "PESSOAL OPER"
        cond_oper_armazem = df["Centro Custo"] == "Operação Armazenagem"
        cond_outros_armazem = df["Centro Custo"] != "Operação Armazenagem"

        df.loc[cond_farma_direto & cond_pessoal_oper & cond_oper_armazem, "Tabela"] = "Folha Razão"
        df.loc[cond_farma_direto & cond_pessoal_oper & cond_outros_armazem, "Tabela"] = "Folha Razão Outros"
        df.loc[cond_farma_indireto & cond_pessoal_oper, "Tabela"] = "Rateio Indiretos Operações"
        
        st.dataframe(df)
        filtro = (
            (df["Tabela_Consolidada"] == "Farma Direto") &
            (df["Grupo"] == "PESSOAL OPER") &
            (df["Centro Custo"] == "Operação Armazenagem")
        )

        df_2 = df.loc[filtro].copy()
        st.dataframe(df_2)
        # Terceiros
        cond_terceiros_oper = df["Grupo"] == "TERCEIROS OPER"
        cond_conta_temp = df["Conta"] == "60301020209"

        df.loc[cond_farma_direto & cond_terceiros_oper, "Tabela"] = "Custos Operacionais"
        df.loc[cond_farma_indireto & cond_conta_temp, "Tabela"] = "Custos Operacionais Indiretos"
        df.loc[cond_farma_direto & cond_conta_temp, "Tabela"] = "Temporarios"
        df.loc[cond_farma_indireto & cond_terceiros_oper, "Tabela"] = "Temporarios Indiretos"

        # Informática, Armazenagem, Outros
        cond_informatica_oper = df["Grupo"].isin(["INFORMATICA OPER", "ARMAZENAGEM OPER", "OUTROS OPER"])

        df.loc[cond_farma_direto & cond_informatica_oper & cond_oper_armazem, "Tabela"] = "Custos Operacionais"
        df.loc[cond_farma_direto & cond_informatica_oper & cond_outros_armazem, "Tabela"] = "Custos Operacionais Outros"
        df.loc[cond_farma_indireto & cond_informatica_oper, "Tabela"] = "Custos Operacionais Indiretos"

        # Indenização
        cond_inden = df["Grupo"] == "INDEN.MERCADORIAS"
        df.loc[cond_farma_direto & cond_inden & cond_oper_armazem, "Tabela"] = "Indenização de Mercadorias"

        # Descontos
        cond_descontos = df["Grupo"] == "DESCONTOS"
        df.loc[cond_farma_direto & cond_descontos & cond_oper_armazem, "Tabela"] = "Descontos"

        df=df.rename(columns={"Grupo":"Area"})
        df=df.rename(columns={"Sigla":"Grupo"})
        df["Tabela"] = df["Tabela"].fillna("Tabela Desconhecida")

        df = df.groupby(["Tabela", "Ano", "Mês", "Filial UF", "Area", "Grupo", "Item"], as_index=False)["saldo"].sum()
        # Atualiza o session_state
        st.session_state["Razao_Farma_Consolidado"] = df.copy()
        st.session_state["concluidos"].append("Razão - Custos Alocados - Adicionado")
        return df.copy()

    @classmethod
    def consolidado(cls):
        if "resultado_final" not in st.session_state:

            st.session_state["concluidos"] = []
            for i in st.session_state["concluidos"]:
                st.write(i)


            st.session_state["nas_depara"] = pd.DataFrame()

            st.session_state["nas_depara_razao"] = pd.DataFrame()        

            st.session_state["tamanhos"] = []        


            df_embalagens = cls.Embalagem_Adequa()
            st.dataframe(df_embalagens)

            df_overhead = cls.Overhead()
            st.dataframe(df_overhead)

            cls.farma_direto_indireto()
            
            df_custos_alocados = cls.custos_alocados()
            st.dataframe(df_custos_alocados)
            

            Relatorios_Rateio.carregar_volume()

            Relatorios_Rateio.adequacao()

            Relatorios_Rateio.insumos()

            Relatorios_Rateio.faturamento()

            Relatorios_Rateio.ocupacao_armazem()

            # Lista de dataframes auxiliares do session_state
            dfs_adicionais = [
                st.session_state.get("caminhos_arquivos_volumes", pd.DataFrame()),
                st.session_state.get("caminhos_arquivos_adequacao", pd.DataFrame()),
                st.session_state.get("caminhos_arquivos_insumos", pd.DataFrame()),
                st.session_state.get("caminhos_arquivos_faturamento", pd.DataFrame()),
                st.session_state.get("caminhos_arquivos_ocupacao", pd.DataFrame()),
            ]


            # Garante que todos os objetos são DataFrames válidos
            dfs_adicionais = [df if isinstance(df, pd.DataFrame) else pd.DataFrame() for df in dfs_adicionais]

            # Concatena todos os dataframes em um único consolidado
            resultado_final = pd.concat(
                [df_embalagens, df_overhead, df_custos_alocados] + dfs_adicionais,
                ignore_index=True
            )
            st.dataframe(resultado_final)
            for col in ["Tabela", "Ano", "Mês", "Filial UF", "Grupo", "Area", "Item"]:
                if resultado_final[col].apply(lambda x: isinstance(x, list)).any():
                    st.write(f"A coluna '{col}' contém listas!")
            resultado_final = resultado_final.groupby([
                "Tabela",
                "Ano",
                "Mês",
                "Filial UF",
                "Grupo",
                "Area",
                "Item"
            ], as_index=False)["saldo"].sum()
            st.dataframe(resultado_final)
            st.session_state["resultado_final"] = resultado_final.copy()

        table_download_consolidados,table_download_consolidados_2,table_download_consolidados_3,table_download_consolidados_4 = st.columns(4)

        st.dataframe(st.session_state["resultado_final"].loc[st.session_state["resultado_final"]["Tabela"] == "Ocupação Armazenagem"], use_container_width=True)
        
        with table_download_consolidados:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                st.session_state["resultado_final"].to_excel(writer, index=False, sheet_name='Consolidado_Rentabilidade')
            
            output.seek(0)
            st.download_button(
                label="📥 Baixar Consolidado - Rentabilidade Razão",
                data=output,
                file_name="Rentabilidade_Armazem.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
        with table_download_consolidados_2:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                st.session_state["Razao_Farma_Consolidado_Para_Download"].to_excel(writer, index=False, sheet_name='Consolidado_DRE')
            
            output.seek(0)
            st.download_button(
                label="📥 Baixar Consolidado - DRE",
                data=output,
                file_name="Consolidado - DRE.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        with table_download_consolidados_3:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                st.session_state["nas_depara_razao"].to_excel(writer, index=False, sheet_name='nas_depara_razao')
            
            output.seek(0)
            st.download_button(
                label="📥 Baixar De Paras Não Encontrados",
                data=output,
                file_name="De Paras Não Encontrados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        with table_download_consolidados_4:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                st.session_state["nas_depara"].to_excel(writer, index=False, sheet_name='nas_depara')
            
            output.seek(0)
            st.download_button(
                label="📥 Baixar De Paras Rateio Não Encontrados",
                data=output,
                file_name="De Paras Rateio Não Encontrados.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

if "arquivos_atualizados" not in st.session_state:
    st.session_state.arquivos_atualizados = []

class Selecionar_Colunas:
    @staticmethod
    def _carregar_colunas():
        return Json.carregar_json_interativo_Colunas_CTC()

    @classmethod
    def colunas_fundamentais(cls):
        return cls._carregar_colunas()["colunas"]["colunas_fundamentais"]

    @classmethod
    def colunas_adicionais(cls):
        return cls._carregar_colunas()["colunas"]["colunas_adicionais"]

    @classmethod
    def colunas_padrao(cls):
        return cls.colunas_fundamentais() + cls.colunas_adicionais()

    @classmethod
    def colunas_totais(cls):
        return cls._carregar_colunas()["colunas_ctc_completa"]["Colunas"]
    
    @classmethod
    def colunas_nao_selecionadas(cls):
        return [col for col in cls.colunas_totais() if col not in cls.colunas_padrao()] 

    @classmethod
    def colunas_farma(cls):
        return cls._carregar_colunas()["colunas"]["colunas_farma"]

    @classmethod
    def df_intec_limpeza_ctc(cls):
        return cls._carregar_colunas()["colunas"]["df_intec_limpeza_ctc"]

    @classmethod
    def df_farma_limpeza_ctc(cls):
        return cls._carregar_colunas()["colunas"]["df_farma_limpeza_ctc"]

    @classmethod
    def df_remover_duplicados(cls):
        return cls._carregar_colunas()["colunas"]["df_remover_duplicados"]

    @classmethod
    def mascara_cob(cls):
        return cls._carregar_colunas()["colunas"]["mascara_cob"]

    @classmethod
    def peso_base(cls):
        return cls._carregar_colunas()["colunas"]["Peso_base"]

    @classmethod
    def colunas_groupby(cls):
        return cls._carregar_colunas()["colunas_groupby"]

    @classmethod
    def colunas_ctc_completa(cls):
        colunas = cls._carregar_colunas()
        return colunas.get("colunas_ctc_completa", [])

class Selecionar_Colunas_Os:
    @staticmethod
    def _carregar_colunas_agregados():
        return Json.carregar_json_interativo()
    
    @classmethod
    def _carregar_colunas_Os_agregados(cls):
        return cls._carregar_colunas_agregados()["dados_os_agregados"]["Colunas"]
    
    @classmethod
    def _carregar_colunas_Os_Frota(cls):
        return cls._carregar_colunas_agregados()["dados_os_frota"]["Colunas"]

    @classmethod
    def _carregar_colunas_Os_Frota_Data(cls):
        return cls._carregar_colunas_agregados()["dados_os_frota"]["Coluna_Data"]

    @classmethod
    def _carregar_colunas_Os_Frota_Ano(cls):
        return cls._carregar_colunas_agregados()["dados_os_frota"]["Ano"][0]
    
    @classmethod
    def _carregar_colunas_Os_356(cls):
        return cls._carregar_colunas_agregados()["dados_df_356"]["Colunas_Os"]
    
    @classmethod
    def _carregar_colunas_CTC_356(cls):
        return cls._carregar_colunas_agregados()["dados_df_356"]["Coluna_CTC"]

class DataCleaner:
    @staticmethod
    def remove_leading_zero_serie(serie):
        serie = serie.fillna('').astype(str).str.strip()
        mask = serie.str.startswith('0')
        serie.loc[mask] = serie.loc[mask].str.lstrip('0').replace('', '0')
        return serie

    @staticmethod
    def clean_number(value):
        if pd.isna(value):
            return 0
        if isinstance(value, str):
            if ',' in value and '.' in value:
                value = value.replace('.', '').replace(',', '.')
            elif ',' in value:
                value = value.replace(',', '.')
            elif '.' in value:
                parts = value.split('.')
                if len(parts[-1]) == 3:
                    value = ''.join(parts[:-1]) + '.' + parts[-1]
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @classmethod
    def clean_currency_serie(cls, serie): # Renomeei para indicar que agora opera em uma série
        serie = serie.astype(str).str.strip().str.replace('R$', '', regex=False)
        serie = serie.apply(cls.clean_number)
        return serie

    @classmethod
    def clean_ctc(cls, df, col):
        return df[col].fillna('0').astype(str).str.strip()

class VariavelFuncoesPadrao:
    @classmethod
    def nomes(cls, nome_arquivo):
        if "arquivos_atualizados" not in st.session_state:
            st.session_state.arquivos_atualizados = []
            st.session_state.arquivos_atualizados.append(nome_arquivo)
        else:
            st.session_state.arquivos_atualizados.append(nome_arquivo)    
    @staticmethod
    def atualzar_dados_ctc():
        st.write("### Atualizando dados CTC, atenção, isso pode demorar. Recomenda-se fechar outras aplicações e deixar apenas essa aba aberta.")
        if os.path.exists(Json.caminho_json_ctc):
            with open(Json.caminho_json_ctc, "r") as f:
                caminhos_atualizar_ctcs = json.load(f)
        else:
            st.warning("Arquivo JSON nao encontrado. Criando com valores padrao...")
            Json.gerar_json_padrao_ctc()
            with open(Json.caminho_json_ctc, "r") as f:
                caminhos_atualizar_ctcs = json.load(f)     

        timestamp = os.path.getmtime(caminhos_atualizar_ctcs["dados_atualizar_ctc"]["path_output"])
        data_modificacao = datetime.fromtimestamp(timestamp)
        if data_modificacao.date() == date.today():
            st.success("✔️ Arquivo atualizado hoje. Não é necessário atualizar.")
        else:
            st.warning(f"⚠️ Última atualização em: {data_modificacao.strftime('%d/%m/%Y %H:%M:%S')}")
            st.write("### Atenção, caso deseja atualizar, isso pode demorar de 5 a 10 minutos. Recomenda-se fechar outras aplicações e deixar apenas essa aba aberta.")
        
        # Adiciona o campo para o usuário selecionar o ano
        ano_selecionado = st.number_input(
            "Selecione o ano para filtrar e gerar o arquivo Parquet:",
            min_value=2020,
            max_value=2030,
            value=2025 # Mantém 2025 como valor padrão
        )

        if st.button("🔄 Aperte para Atualizar"):
            st.write(f"### Atualizando dados CTC para o ano de {ano_selecionado}, isso pode demorar...")
            qvd = caminhos_atualizar_ctcs["dados_atualizar_ctc"]["path"]
            caminho_parquet = caminhos_atualizar_ctcs["dados_atualizar_ctc"]["path_output"]
            
            st.write("### Carregando Dados do banco do B.I")
            
            # Carrega o QVD completo para manter todas as colunas
            df = qvd_reader.read(qvd)

            # Converte a coluna de data e filtra pelo ano selecionado pelo usuário
            df["DataCTC"] = pd.to_datetime(df["DataCTC"], errors="coerce", dayfirst=True)
            df_filtrado = df[df["DataCTC"].dt.year == ano_selecionado].copy()

            # Verifica se o filtro resultou em dados para evitar criar um arquivo vazio
            if df_filtrado.empty:
                st.error(f"Nenhum dado encontrado para o ano {ano_selecionado}. O arquivo Parquet não foi atualizado.")
                return

            st.write("### Salvando arquivo Parquet otimizado para a aplicação...")
            
            df_filtrado.to_parquet(caminho_parquet, compression="snappy")
            
            st.write("### Dados Atualizados.")

            todas_colunas = df_filtrado.columns.tolist()

            st.write("### Salvando informações das colunas no JSON...")
            if os.path.exists(Json.caminho_json_ctc_colunas):
                with open(Json.caminho_json_ctc_colunas, "r", encoding="utf-8") as f:
                    dados_colunas_existente = json.load(f)
            else:
                # Corrigido para chamar a função correta
                Json.gerar_json_colunas_padrao_ctc()
                with open(Json.caminho_json_ctc_colunas, "r", encoding="utf-8") as f:
                    dados_colunas_existente = json.load(f)
            
            # Atualiza a lista de colunas no JSON e salva
            dados_colunas_existente["colunas_ctc_completa"]["Colunas"] = todas_colunas
            with open(Json.caminho_json_ctc_colunas, "w", encoding="utf-8") as f:
                json.dump(dados_colunas_existente, f, ensure_ascii=False, indent=4)

            st.success(f"### Atualização para o ano de {ano_selecionado} foi Finalizada :)")    

            # Limpeza de memória
            del df
            del df_filtrado
            gc.collect()
            
    @staticmethod
    def atualzar_dados_os():
        st.write("### Atualizando dados CTC, atenção, isso pode demorar. Recomenda-se fechar outras aplicações e deixar apenas essa aba aberta.")
        if os.path.exists(Json.caminho_json_ctc):
            with open(Json.caminho_json_ctc, "r") as f:
                caminhos_atualizar_ctcs = json.load(f)
        else:
            st.warning("Arquivo JSON nao encontrado. Criando com valores padrao...")
            Json.gerar_json_padrao_ctc()
            with open(Json.caminho_json_ctc, "r") as f:
                caminhos_atualizar_ctcs = json.load(f)     
        try:
            timestamp = os.path.getmtime(caminhos_atualizar_ctcs["dados_atualizar_ctc"]["path_os_output"])
            data_modificacao = datetime.fromtimestamp(timestamp)
            if data_modificacao.date() == date.today():
                st.success("✔️ Arquivo atualizado hoje. Não é necessário atualizar.")
            else:
                st.warning(f"⚠️ Última atualização em: {data_modificacao.strftime('%d/%m/%Y %H:%M:%S')}")
                st.write("### Atenção, caso deseja atualizar, isso pode demorar de 5 a 10 minutos. Recomenda-se fechar outras aplicações e deixar apenas essa aba aberta.")
        except:
            pass

        if st.button("🔄 Aperte para Atualizar"):
            st.write("### Atualizando dados CTC, atenção, isso pode demorar. Recomenda-se fechar outras aplicações e deixar apenas essa aba aberta.")
            qvd = caminhos_atualizar_ctcs["dados_atualizar_ctc"]["path_os"]
            caminho_parquet = caminhos_atualizar_ctcs["dados_atualizar_ctc"]["path_os_output"]
            
            st.write("### Carregando Dados do banco do B.I")
            
            df = qvd_reader.read(qvd)

            st.write("### Atualizando dados CTC para aplicação.")
            
            df.to_parquet(caminho_parquet, compression="snappy")
            
            st.write("### Dados Atualizados.")
    @staticmethod
    def atualzar_dados_os_base():
        st.write("### Atualizando dados CTC, atenção, isso pode demorar. Recomenda-se fechar outras aplicações e deixar apenas essa aba aberta.")
        if os.path.exists(Json.caminho_json_ctc):
            with open(Json.caminho_json_ctc, "r") as f:
                caminhos_atualizar_ctcs = json.load(f)
        else:
            st.warning("Arquivo JSON nao encontrado. Criando com valores padrao...")
            Json.gerar_json_padrao_ctc()
            with open(Json.caminho_json_ctc, "r") as f:
                caminhos_atualizar_ctcs = json.load(f)     
        try:    
            timestamp = os.path.getmtime(caminhos_atualizar_ctcs["dados_atualizar_ctc"]["path_base_output"])
            data_modificacao = datetime.fromtimestamp(timestamp)
            if data_modificacao.date() == date.today():
                st.success("✔️ Arquivo atualizado hoje. Não é necessário atualizar.")
            else:
                st.warning(f"⚠️ Última atualização em: {data_modificacao.strftime('%d/%m/%Y %H:%M:%S')}")
                st.write("### Atenção, caso deseja atualizar, isso pode demorar de 5 a 10 minutos. Recomenda-se fechar outras aplicações e deixar apenas essa aba aberta.")
        except:
            pass

        if st.button("🔄 Aperte para Atualizar"):
            st.write("### Atualizando dados CTC, atenção, isso pode demorar. Recomenda-se fechar outras aplicações e deixar apenas essa aba aberta.")
            qvd = caminhos_atualizar_ctcs["dados_atualizar_ctc"]["path_base"]
            caminho_parquet = caminhos_atualizar_ctcs["dados_atualizar_ctc"]["path_base_output"]
            
            st.write("### Carregando Dados do banco do B.I")
            
            df = qvd_reader.read(qvd)

            st.write("### Atualizando dados CTC para aplicação.")
            
            df.to_parquet(caminho_parquet, compression="snappy")
            
            st.write("### Dados Atualizados.")
    
    @staticmethod
    def update_arquivo_excel():

        if "fase" not in st.session_state:
            st.session_state["fase"] = "filtro"

        # Carregar JSONs necessários
        if "files" not in st.session_state:
            if os.path.exists(Json.caminho_json_ctc):
                with open(Json.caminho_json_ctc, "r", encoding="utf-8") as f:
                    atualzar_parquet = json.load(f)
            else:
                st.warning("Arquivo JSON nao encontrado. Criando com valores padrao...")
                Json.gerar_json_padrao_ctc()
                with open(Json.caminho_json_ctc, "r", encoding="utf-8") as f:
                    atualzar_parquet = json.load(f)

            if not os.path.exists(Json.caminho_json_ctc_colunas):
                st.warning("Arquivo JSON de colunas não encontrado. Criando com valores padrão...")
                Json.gerar_json_colunas_padrao_ctc()
                with open(Json.caminho_json_ctc_colunas, "r", encoding="utf-8") as f:
                    dados_colunas = json.load(f)
            else:
                with open(Json.caminho_json_ctc_colunas, "r", encoding="utf-8") as f:
                    dados_colunas = json.load(f)

            if "colunas_ano_mes" not in dados_colunas:
                Json.json_ano_data()
                with open(Json.caminho_json_ctc_colunas, "r", encoding="utf-8") as f:
                    dados_colunas = json.load(f)

            anos_meses_disponiveis = dados_colunas.get("colunas_ano_mes", {})

        # --- FASE DE FILTRO ---
        if st.session_state["fase"] == "filtro":
            if not anos_meses_disponiveis:
                st.warning("⚠️ Dicionário de ano e mês não encontrado no JSON.")
                return

            ano_selecionado = st.multiselect("Selecione o Ano:", sorted(key for key in anos_meses_disponiveis.keys()))
            
            meses_disponiveis = sorted(set(
                mes 
                for ano in ano_selecionado 
                for mes in anos_meses_disponiveis.get(ano, [])
            ))

            mes_selecionado = st.multiselect("Selecione os Meses:", meses_disponiveis, default=meses_disponiveis)

            if st.button("🔄 Continuar"):
                caminho_parquet = atualzar_parquet["dados_atualizar_parquet"]["path"]
                df = pd.read_parquet(caminho_parquet)

                if "DataCTC" not in df.columns:
                    st.warning("Coluna nao encontrada, selecione uma coluna valida para atualizar")
                    coluna_valida = st.selectbox("Colunas", df.columns)
                    df = df.rename(columns={coluna_valida: "DataCTC"})

                df["DataCTC"] = pd.to_datetime(df["DataCTC"], errors="coerce",dayfirst=True)
                df["Ano"] = df["DataCTC"].dt.year.astype(int)
                df["Mês"] = df["DataCTC"].dt.month.astype(int)

                #ano_selecionado = [int(ano) for ano in ano_selecionado]
                #mes_selecionado = [int(mes) for mes in mes_selecionado]

                df_filtrado = df[
                    (df["Ano"].isin([int(ano) for ano in ano_selecionado])) &
                    (df["Mês"].isin([int(mes) for mes in mes_selecionado]))
                ].copy()

                st.session_state["dados"] = df_filtrado
                st.session_state["fase"] = "colunas"
                st.rerun()

        # --- FASE DE SELEÇÃO DE COLUNAS E PROCESSAMENTO ---
        elif st.session_state["fase"] == "colunas":
            df = st.session_state["dados"]

            col_arquivo_padrao1, col_arquivo_padrao2, col_arquivo_padrao3 = st.columns(3)

            colunas_nao_selecionadas = Selecionar_Colunas.colunas_nao_selecionadas()
            colunas_fundamentais = Selecionar_Colunas.colunas_fundamentais()
            colunas_adicionais = Selecionar_Colunas.colunas_adicionais()
            colunas_adicionais_ativas = []

            with col_arquivo_padrao1:
                st.markdown("###### 🔧 Colunas sempre ativas e não removiveis")
                with st.container(height=500, border=True):
                    for col in colunas_fundamentais:
                        st.write(col)

            with col_arquivo_padrao2:
                st.markdown("###### 🔧 Colunas Ativas por Padrão mas removiveis")
                with st.container(height=500, border=True):
                    for col in colunas_adicionais:
                        if st.toggle(f"Ativar coluna: {col}", value=True):
                            colunas_adicionais_ativas.append(col)

            with col_arquivo_padrao3:
                st.markdown("###### ➕ Ativar Colunas Adicionais")
                with st.container(height=500, border=True):
                    for col in colunas_nao_selecionadas:
                        if st.toggle(f"Ativar coluna: {col}", value=False):
                            colunas_adicionais_ativas.append(col)

            st.dataframe(df.head(25))

            if st.button("🔄 Aperte para Continuar"):
                todas_as_colunas = colunas_fundamentais + colunas_adicionais_ativas
                colunas_disponiveis = [col for col in todas_as_colunas if col in df.columns]
                colunas_indisponiveis = [col for col in todas_as_colunas if col not in df.columns]

                if colunas_indisponiveis:
                    st.warning(f"Colunas indisponíveis: {colunas_indisponiveis}")

                df = df[colunas_disponiveis].loc[df["Status Ocorrencia"] != "Cancelado"]
                df = df.fillna("Desconsiderar")

                df_farma = df[df["Banco"] == "Farma"][Selecionar_Colunas.colunas_farma()].copy()
                df = df[df["Banco"] == "Intec"]

                df.drop_duplicates(subset=Selecionar_Colunas.colunas_fundamentais(), keep="first", inplace=True)
                df = df.drop(columns=["Banco", "corresp"])

                df_farma.drop_duplicates(subset=["CTC"], keep="first", inplace=True)
                df_farma = df_farma.drop(columns=["NotaFiscal"])

                df = df.rename(columns={
                    "Consignatario": "Consignatario Intec",
                    "Valor Frete Bruto": "Valor Frete Bruto Intec",
                    "CNPJ Consig": "CNPJ Consig Intec"
                })

                st.write("Limpeza")
                for col in Selecionar_Colunas.df_intec_limpeza_ctc():
                    if col in df.columns:
                        df[col] = DataCleaner.clean_ctc(df, col)
                        df[col] = DataCleaner.remove_leading_zero_serie(df[col])
                for col in Selecionar_Colunas.df_farma_limpeza_ctc():
                    if col in df_farma.columns:
                        df_farma[col] = DataCleaner.clean_ctc(df_farma, col)
                        df_farma[col] = DataCleaner.remove_leading_zero_serie(df_farma[col])

                df_farma = df_farma.rename(columns={
                    "Consignatario": "Consignatario Farma",
                    "CNPJ Consig": "CNPJ Consig Farma",
                    "Valor Frete Bruto": "Valor Frete Bruto Farma",
                    "corresp": "CTC_"
                }).rename(columns={"CTC": "corresp", "CTC_": "CTC"})

                df = df.merge(df_farma, how="left", on=["CTC"])

                del df_farma
                gc.collect()

                df["Consignatario"] = df["Consignatario Farma"].fillna(df["Consignatario Intec"])
                df["CNPJ Consig"] = df["CNPJ Consig Farma"].fillna(df["CNPJ Consig Intec"])

                df["Banco"] = df["Banco"].fillna("Intec")
                df["Valor Frete Bruto"] = df["Valor Frete Bruto Farma"].fillna(df["Valor Frete Bruto Intec"])
                
                colunas_monetarias = [
                    "Valor Frete Bruto","Valor Frete Bruto Farma", "Valor Frete Bruto Intec", 
                    "Peso", "Peso Cubado", "Peso NF", "Valor NF", "Volumes NF"
                ]
                for col in colunas_monetarias:
                    if col in df.columns:
                        df[col] = DataCleaner.clean_currency_serie(df[col])

                mask_cob = df["Tipo Documento"] == "COB"
                df.loc[mask_cob, Selecionar_Colunas.mascara_cob()] = 0
                df["Peso Base"] = df[Selecionar_Colunas.peso_base()].max(axis=1)

                df["Nota Fiscal"] = df["NotaFiscal"].astype(str) + "-" + df["Serie"].astype(str)

                Concatenar_Nfs_por_Ctcs = df[["CTC", "Nota Fiscal"]].groupby('CTC', as_index=False).agg({
                    'Nota Fiscal': lambda x: ', '.join(x)
                })

                df = df.drop(columns=["Nota Fiscal"])
                df = df.merge(Concatenar_Nfs_por_Ctcs, how="left", on=["CTC"])

                colunas_numericas = [
                    "NotaFiscal", "Serie", "Valor Frete Bruto", "Peso", "Peso NF", "Volumes NF",
                    "Valor NF", "Peso Cubado", "Peso Base"
                ]

                colunas_agrupar = [col for col in df.columns if col in colunas_numericas]
                df.drop_duplicates(subset=["CTC", "NotaFiscal"], keep="first", inplace=True)
                df = df.fillna("N/D")

                df_ctc = df.groupby("CTC", as_index=False).agg(Selecionar_Colunas.colunas_groupby())
                df = df.drop(columns=colunas_agrupar)

                df = df.merge(df_ctc, how="left", on=["CTC"])
                # Remove duplicados finais
                if "CTC" in Selecionar_Colunas.df_remover_duplicados():
                    df.drop_duplicates(subset=["CTC"], keep="first", inplace=True)

                df.columns = df.columns.str.strip()
                df = df.replace('N/D', pd.NA)
                # Atualiza st.session_state com nomes importantes
                st.session_state["dados"] = df
                st.session_state["df_fun"] = df.copy()

                st.dataframe(st.session_state["df_fun"].head(25))
                st.session_state["fase"] = "download"
                VariavelFuncoesPadrao.nomes("qvd_file")
        elif st.session_state["fase"] == "download":
            if st.button("🔄 Clique para Habilitar o Download"):
                    # Exportação para Excel .xlsx
                    buffer = io.BytesIO()
                    st.session_state["df_fun"].to_excel(buffer, index=False, engine='openpyxl')
                    buffer.seek(0)
                    st.download_button(
                        label="📥 Baixar Arquivo",
                        data=buffer,
                        file_name=f"dados_filtrados.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    @staticmethod
    def update_arquivos_os_agregados():
        nome_tabela = "dados_os_agregados"

        if nome_tabela not in st.session_state:
            df = Json.carregar_dados_por_nome(nome_tabela)
            df.columns = df.columns.str.strip()

            st.session_state[nome_tabela] = df
            st.success("Agregados atualizados")
            VariavelFuncoesPadrao.nomes(nome_tabela)
    @staticmethod
    def update_arquivos_os_frota():
        nome_tabela = "dados_os_frota"
        
        if nome_tabela not in st.session_state:
            df = Json.carregar_dados_por_nome(nome_tabela)
            df.columns = df.columns.str.strip()

            st.session_state[nome_tabela] = df
            st.success("Frota atualizada")
            VariavelFuncoesPadrao.nomes(nome_tabela)
    @staticmethod
    def update_arquivo_356():
        nome_tabela = "dados_df_356"

        if nome_tabela not in st.session_state:
            df = Json.carregar_dados_por_nome(nome_tabela)
            df.columns = df.columns.str.strip()
            
            df["CTC"] = df["CTC"].fillna(0).astype(float).astype("int64").astype(str).str.strip()
            
            colunas = ["CTC", "Nro OS"]
            for col in colunas:
                df[col] = DataCleaner.clean_ctc(df, col)
                df[col] = DataCleaner.remove_leading_zero_serie(df[col])
            
            df["Valor Frete"] = df["Valor Frete"].apply(DataCleaner.clean_number)

            st.session_state[nome_tabela] = df
            st.success("356 atualizada")
            VariavelFuncoesPadrao.nomes(nome_tabela)
    @classmethod
    def uploads(cls):

        Json.carregar_json_interativo()

        VariavelFuncoesPadrao.update_arquivo_356()
        VariavelFuncoesPadrao.update_arquivos_os_agregados()
        VariavelFuncoesPadrao.update_arquivos_os_frota()
        VariavelFuncoesPadrao.update_arquivo_excel()

        col_agregados = Selecionar_Colunas_Os._carregar_colunas_Os_agregados()
        col_frota = Selecionar_Colunas_Os._carregar_colunas_Os_Frota()
        col_frota_data = Selecionar_Colunas_Os._carregar_colunas_Os_Frota_Data()
        col_356 = Selecionar_Colunas_Os._carregar_colunas_Os_356()
        col_356_ctc = Selecionar_Colunas_Os._carregar_colunas_CTC_356()
        
        ######### Agregados

        df_agregados = st.session_state.dados_os_agregados.copy()
        if any(col in df_agregados.columns for col in col_agregados) == True:
            pass
        else:
            st.write("Coluna de Os não encontrada, selecione uma coluna válida para atualizar")
            col_agregados = st.multiselect("Coluna de Os", df_agregados.columns, key="coluna_os_agregados")

            if col_agregados:
                Json.atualizar_json_colunas_os_agregados(col_agregados)
                st.success("Colunas atualizadas com sucesso no JSON!")

        col_agregados = col_agregados[0]
        df_agregados = df_agregados.rename(columns={col_agregados: "Nro OS"})
        df_agregados["Nro OS"] = DataCleaner.clean_ctc(df_agregados, "Nro OS")
        df_agregados["Nro OS"] = DataCleaner.remove_leading_zero_serie(df_agregados["Nro OS"])

        ######### Frota
        df_frota = st.session_state.dados_os_frota.copy()

        if any(col in df_frota.columns for col in col_frota) == True:
            col_encontrada = next((col for col in col_frota if col in df_frota.columns), None)
            pass
        else:
            st.write("Coluna de Os não encontrada, selecione uma coluna válida para atualizar")
            col_encontrada = st.multiselect("Coluna de Os", df_frota.columns, key="coluna_os_frota")

            if col_encontrada:
                Json.atualizar_json_colunas_os_agregados(col_encontrada)
                st.success("Colunas atualizadas com sucesso no JSON!")

        col_frota = col_encontrada
        st.write(col_frota)
        df_frota = df_frota.rename(columns={col_frota: "Nro OS"})
        st.dataframe(df_frota)
        try:
            df_frota["Nro OS"] = DataCleaner.clean_ctc(df_frota, "Nro OS")
        except:
            pass
        try:
            df_frota["Nro OS"] = DataCleaner.remove_leading_zero_serie(df_frota["Nro OS"])
        except:
            pass

        if any(col in df_frota.columns for col in col_frota_data) == True:
            pass
        else:
            st.write("Coluna da Data de Emissão não encontrada, selecione uma coluna válida para atualizar")
            col_frota_data = st.multiselect("Coluna de Os", df_frota.columns, key="coluna_data_os_agregados")
            if col_frota_data:
                Json.atualizar_json_colunas_os_frota_data_emissao(col_frota_data)
                st.success("Colunas atualizadas com sucesso no JSON!")

        col_frota_data = col_frota_data[0]
        df_frota[col_frota_data] = pd.to_datetime(df_frota[col_frota_data], format="%d/%m/%Y", errors="coerce")
        df_frota = df_frota.loc[df_frota[col_frota_data].dt.year >= Selecionar_Colunas_Os._carregar_colunas_Os_Frota_Ano()]


        ######### 356
        df_356 = st.session_state.dados_df_356.copy()

        if any(col in df_356.columns for col in col_356) == True:
            pass
        else:
            st.write("Coluna de Os não encontrada, selecione uma coluna válida para atualizar")
            col_356 = st.multiselect("Coluna de Os", df_356.columns, key="coluna_os_agregados")
            if col_356:
                Json.atualizar_json_colunas_os_356_Os(col_356)
                st.success("Colunas atualizadas com sucesso no JSON!")
    
        if any(col in df_356.columns for col in col_356_ctc) == True:
            pass
        else:
            st.write("Coluna de CTC não encontrada, selecione uma coluna válida para atualizar")
            col_356_ctc = st.multiselect("Coluna de Os", df_356.columns, key="col_356_selecionado_ctc")
            if col_356_ctc:
                Json.atualizar_json_colunas_os_356_CTC(col_356_ctc)
                st.success("Colunas atualizadas com sucesso no JSON!")

        col_356 = col_356[0]
        col_356_ctc = col_356_ctc[0]

        df_356 = df_356.rename(columns={col_356: "Nro OS", col_356_ctc: "CTC"})
        receita_os = df_356[["Nro OS","CTC"]].merge(st.session_state.df_fun[["CTC", "Valor Frete Bruto"]], how="left", on="CTC")
        receita_os = receita_os.groupby([col_356], as_index=False).agg({"Valor Frete Bruto": "sum"})

        receita_agregado = df_agregados[["Nro OS"]].merge(receita_os, how="left", on="Nro OS")
        receita_frota = df_frota[["Nro OS"]].merge(receita_os, how="left", on="Nro OS")

        st.success("📈 Receita consolidada gerada com sucesso!")

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            receita_agregado.to_excel(writer, index=False, sheet_name='Receita Agregados')
            receita_frota.to_excel(writer, index=False, sheet_name='Receita Frota')
            receita_os.to_excel(writer, index=False, sheet_name='Receita Os')
        output.seek(0)

        st.download_button(
            label="📥 Baixar Receita Consolidada (Excel)",
            data=output,
            file_name="receita_consolidada.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    @classmethod
    def base(cls):
        if "df_fun" not in st.session_state:
            cls.update_arquivo_excel()
            arquivo_base = st.session_state.df_fun.copy()
        else:
            arquivo_base = st.session_state.df_fun.copy()
        
        arquivo_base.columns = arquivo_base.columns.str.strip()
        st.dataframe(arquivo_base.head(10))

        with open(Json.caminho_json_ctc_colunas, "r",encoding="utf-8") as f:
            dados_colunas = json.load(f)
            dados_colunas = dados_colunas["colunas_ctc_base"]

        for col in dados_colunas["colu_cnpj"]:
            if col not in arquivo_base.columns:
                st.warning(f"A coluna {col} nao foi encontrada no arquivo.")
            else:
                arquivo_base[col] = arquivo_base[col].str[:8]
                if col == "CNPJ Remetente":
                    arquivo_base.rename(columns={col: "CNPJ Raiz - Remetente"}, inplace=True)
                elif col == "CNPJ Dest":
                    arquivo_base.rename(columns={col: "CNPJ Raiz - Dest"}, inplace=True)

        for col in dados_colunas["colu_data"]:
            if col not in arquivo_base.columns:
                st.warning(f"A coluna {col} nao foi encontrada no arquivo.")
            else:
                arquivo_base[col] = pd.to_datetime(arquivo_base[col], format="%d/%m/%Y", errors="coerce")
        
        colunas_encontradas = []
        for col in dados_colunas["colunas_padrao"]:
            if col not in arquivo_base.columns:
                st.warning(f"A coluna {col} nao foi encontrada no arquivo.")
            else:
                colunas_encontradas.append(col)
        arquivo_base = arquivo_base[colunas_encontradas]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            arquivo_base.to_excel(writer, index=False, sheet_name='Arquivo Base')
        
        output.seek(0)

        st.download_button(
            label="📥 Baixar Arquivos de Base",
            data=output,
            file_name="arquivo_base.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    @classmethod
    def ecobocox(cls):
        # Carrega o DataFrame principal do session_state
        if "df_fun" not in st.session_state:
            cls.update_arquivo_excel()
        
        arquivo_ecobox = st.session_state.df_fun.copy()
        st.dataframe(arquivo_ecobox)

        # Padroniza os nomes das colunas
        arquivo_ecobox.columns = arquivo_ecobox.columns.str.strip()

        # Carrega as regras de negócio das colunas necessárias para o Ecobox
        with open(Json.caminho_json_ctc_colunas, "r",encoding="utf-8") as f:
            dados_colunas = json.load(f)
            dados_colunas = dados_colunas["ecobox"]

        # REGRAS DE NEGÓCIO: Seleciona apenas as colunas de entrada relevantes para o cálculo Ecobox
        colunas_existentes = [col for col in dados_colunas["colunas_entrada"] if col in arquivo_ecobox.columns]
        colunas_faltantes = [col for col in dados_colunas["colunas_entrada"] if col not in arquivo_ecobox.columns]

        arquivo_ecobox = arquivo_ecobox[colunas_existentes]
        # REGRAS DE NEGÓCIO: Remove duplicatas de CTC para garantir unicidade
        arquivo_ecobox = arquivo_ecobox.drop_duplicates(subset=["CTC"])

        # Carrega tabelas auxiliares de Natureza e Dimensões Ecobox
        if os.path.exists(Json.caminho_json_natureza_ecobox):
            natureza = pd.read_excel(Json.caminho_json_natureza_ecobox)
            natureza.columns = natureza.columns.str.strip()
        else:
            natureza = pd.DataFrame(**dados_colunas["naturezas_ecobox"])
            natureza.to_excel(Json.caminho_json_natureza_ecobox, index=False)

        if os.path.exists(Json.caminho_json_dimensoes_ecobox):
            dimensoes = pd.read_excel(Json.caminho_json_dimensoes_ecobox)
            dimensoes.columns = dimensoes.columns.str.strip()
        else:
            # Carrega as dimensões do JSON (garante que as datas estão corretas)
            dimensoes_json = dados_colunas["arquivo_preços"]["data"]
            dimensoes_columns = dados_colunas["arquivo_preços"]["columns"]
            dimensoes = pd.DataFrame(dimensoes_json, columns=dimensoes_columns)

            # Converte as datas para o padrão ISO (YYYY-MM-DD)
            for col_data in ["Inicio Vigência", "Fim Vigência"]:
                if col_data in dimensoes.columns:
                    dimensoes[col_data] = pd.to_datetime(dimensoes[col_data], errors='coerce')
                    dimensoes[col_data] = dimensoes[col_data].dt.strftime('%Y-%m-%d')

            # Salva o arquivo Excel com as datas corretas
            dimensoes.to_excel(Json.caminho_json_dimensoes_ecobox, index=False)
        
        # REGRAS DE NEGÓCIO: Ajusta datas para garantir que estejam no formato correto
        for col in dimensoes.columns:
            if col in ["Inicio Vigência", "Fim Vigência"]:
                try:
                    dimensoes[col] = dimensoes[col].apply(
                        lambda x: pd.NA if isinstance(x, str) and "SUBSTITUIR" in x else x)
                    dimensoes[col] = dimensoes[col].fillna(datetime.today())
                    dimensoes[col] = pd.to_datetime(dimensoes[col], format="mixed", errors="coerce", dayfirst=True)
                except Exception as e:
                    st.write(e)
            elif col in ["Dimenssão_"]:
                try:
                    dimensoes[col] = dimensoes[col].str.strip()
                except Exception as e:
                    st.write(e)
            else:
                pass

        # REGRAS DE NEGÓCIO: Padroniza coluna Natureza para facilitar o merge
        for col in natureza.columns:
            try:
                natureza[col] = natureza[col].str.strip()
            except:
                pass

        # REGRAS DE NEGÓCIO: Filtra apenas linhas que são Ecobox
        arquivo_ecobox = arquivo_ecobox.loc[arquivo_ecobox["Natureza"].str.contains(r"\bEco", case=False, na=False)]
        arquivo_ecobox["Natureza"] = arquivo_ecobox["Natureza"].astype(str).str.strip().str.upper()

        st.dataframe(arquivo_ecobox)

        # REGRAS DE NEGÓCIO: Merge com tabela de Natureza para obter Tipo Ecobox e Dimensão
        if "Natureza" in natureza.columns:
            natureza["Natureza"] = natureza["Natureza"].astype(str).str.strip().str.upper()
        else:
            st.warning(f"A coluna Natureza nao foi encontrada no arquivo de Naturezas no caminho {Json.caminho_json_natureza_ecobox}, disponibilizando arquivo gerado até o momento.")
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                arquivo_ecobox.to_excel(writer, index=False, sheet_name='Arquivo Ecobox')
            output.seek(0)
            st.download_button(
                label="📥 Baixar Arquivos de Ecobox",
                data=output,
                file_name="arquivo_ecobox.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        arquivo_ecobox = arquivo_ecobox.merge(natureza, how='left',on='Natureza')
        arquivo_ecobox = arquivo_ecobox.merge(dimensoes, how='left', on='Dimensão')
        
        # REGRAS DE NEGÓCIO: Filtra apenas registros dentro do período de vigência do preço Ecobox
        arquivo_ecobox = arquivo_ecobox.loc[
            (arquivo_ecobox["DataCTC"] >= arquivo_ecobox["Inicio Vigência"]) & 
            (arquivo_ecobox["DataCTC"] <= arquivo_ecobox["Fim Vigência"])]        

        # REGRAS DE CÁLCULO: Adiciona coluna do valor unitário da Ecobox (sem multiplicar pelos volumes)
        # Regra de cálculo: Mostra o valor unitário da Ecobox conforme tabela de preços
        arquivo_ecobox["Valor Uni. EcoBox Unitário"] = arquivo_ecobox["Valor Uni. EcoBox"]

        # REGRAS DE CÁLCULO: Calcula o valor total do Ecobox (Valor unitário * Volumes)
        # Regra: Multiplica o valor unitário pelo número de volumes para obter o custo total Ecobox
        arquivo_ecobox["Valor Uni. EcoBox"] = arquivo_ecobox["Valor Uni. EcoBox"] * arquivo_ecobox["Volumes NF"]

        # REGRAS DE CÁLCULO: Calcula a liquidez (Valor Frete Bruto - Valor Ecobox)
        # Regra: Subtrai o valor total Ecobox do valor do frete bruto para obter a liquidez
        arquivo_ecobox["Liquidez"] = arquivo_ecobox["Valor Frete Bruto"] - arquivo_ecobox["Valor Uni. EcoBox"]

        st.dataframe(arquivo_ecobox)

        # REGRAS DE NEGÓCIO: Seleciona apenas as colunas de saída relevantes para o relatório Ecobox
        # Adiciona a nova coluna unitária ao relatório
        colunas_saida = dados_colunas["colunas_saida"] + ["Valor Uni. EcoBox Unitário"]
        arquivo_ecobox = arquivo_ecobox[colunas_saida]

        # Exporta o resultado para Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            arquivo_ecobox.to_excel(writer, index=False, sheet_name='Arquivo Ecobox')
        
        output.seek(0)

        st.download_button(
            label="📥 Baixar Arquivos de Ecobox",
            data=output,
            file_name="arquivo_ecobox.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
# Interface Streamlit
Funções_padrao = [
                        "🔢 Atualizar CTCs",
                        "🔢 Atualizar Datas",
                        "🔢 Consolidar CTCs - Fluxo Os",
                        "🔢 Consolidar Receitas",
                        "🔢 Consolidar Bases",
                        "🔢 Consolidar Ecobox",
                        "🔢 Consolidar Rentabilidade_Armazem",
                        "🔢 Atualizar Os",
                        "🔢 Atualizar Os_Base"
                        ]

mapa_funcoes_tabela = {
    "🔢 Atualizar CTCs": {"func": VariavelFuncoesPadrao.atualzar_dados_ctc},
    "🔢 Atualizar Os": {"func": VariavelFuncoesPadrao.atualzar_dados_os},
    "🔢 Atualizar Os_Base": {"func": VariavelFuncoesPadrao.atualzar_dados_os_base},
    "🔢 Atualizar Datas": {"func": Json.json_ano_data},
    "🔢 Consolidar CTCs - Fluxo Os": {"func": VariavelFuncoesPadrao.update_arquivo_excel},
    "🔢 Consolidar Bases": {"func": VariavelFuncoesPadrao.base},
    "🔢 Consolidar Receitas": {"func": VariavelFuncoesPadrao.uploads},
    "🔢 Consolidar Ecobox": {"func": VariavelFuncoesPadrao.ecobocox},
    "🔢 Consolidar Rentabilidade_Armazem": {"func": Relatorios_DRE.consolidado}
}

##############################

# Página principal com os 3 botões
def pagina_principal():
    st.title("Alteryx da Shopee - Hub de Aplicativos")

    st.subheader("Escolha uma opção:")
    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📁 Navegar em Pastas"):
            st.session_state['pagina'] = 'pastas'
            st.rerun()

    with col2:
        if st.button("⚙️ Automatizadores Avançadas"):
            st.session_state['pagina'] = 'processo_2'
            st.rerun()

    with col3:
        if st.button("🧪 Hub - Relatórios Padrão"):
            st.session_state['pagina'] = 'processo_3'
            st.rerun()

# Função para navegar em pastas
def navegar_pastas():
    st.title("📁 Navegador de Pastas")

    caminho_atual = st.session_state.get('caminho_atual', st.session_state['caminho_raiz'])
    conteudo = os.listdir(caminho_atual)
    conteudo.sort()

    st.markdown(f"### Conteúdo de: `{caminho_atual}`")

    for item in conteudo:
        caminho_item = os.path.join(caminho_atual, item)

        if os.path.isdir(caminho_item):
            if st.button(f"📁 Abrir {item}"):
                st.session_state['caminho_atual'] = caminho_item
                st.rerun()
        else:
            st.markdown(f"📄 {item}")

    if caminho_atual != st.session_state['caminho_raiz']:
        if st.button("⬅️ Voltar"):
            st.session_state['caminho_atual'] = os.path.dirname(caminho_atual)
            st.rerun()

    if st.button("🏠 Voltar à página principal"):
        st.session_state['pagina'] = 'principal'
        st.rerun()

# Função para processar arquivos
def processo_2():
    st.title("⚙️ Tratamentos de Arquivos")
    st.markdown("### Selecione o arquivo que deseja tratar")

    # Upload do arquivo
    st.write("📁 Pasta atual: Deseja fazer o Upload de um arquivo manual ou usar um arquivo padrão?")
    
    option = st.selectbox(
        "📁 Pasta atual: Deseja fazer o Upload de um arquivo manual ou usar um arquivo padrão?",
        ("Padrão", "Upload"),
        index=None,
        placeholder="Selecione uma Opção")
    
    if option == "Upload":
        arquivo = st.file_uploader("📄 Faça upload do arquivo Excel ou PDF", type=["xlsx", "xls", "pdf"])

    else:
        arquivo = VariavelFuncoesPadrao.update_arquivo_excel()

    with st.sidebar:
        st.subheader("📜 Histórico de Tratamentos")
        with st.expander("📈 Importar Histórico"):
            historico.importar_historico()

        with st.expander("📈 Histórico de Edições"):    
            for i, passo in enumerate(Historico().mostrar_historico()):
                st.markdown(f"{i+1}. **{passo['acao']}** → `{passo['params']}`")
                
        with st.expander("📈 Exportar Histórico"):
            if "historico_passos" not in st.session_state or st.session_state["historico_passos"] == []:
                st.write("Nenhum histórico para exportar.")
            else:
                st.session_state["json_historico"] = json.dumps(
                st.session_state.get("historico_passos", []).copy(), 
                indent=2)

                st.download_button(
                    label="📥 Baixar Histórico",
                    data=st.session_state["json_historico"],
                    file_name="historico_transformacoes.json",
                    mime="application/json")

    if arquivo is not None:
        
        Upload_Arquivos(arquivo)
        # 🔄 Reset se for um novo arquivo
        if "nome_arquivo_carregado" not in st.session_state or st.session_state.nome_arquivo_carregado != arquivo.name:
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.nome_arquivo_carregado = arquivo.name

        # Se já há dados carregados
        if "df_fun" in st.session_state:
            st.markdown("### Transformação dos Dados")
            with st.expander("📈 Dados Originais"):
                tab1, tab2, tab3 = st.tabs(["📈 Dados", "🗃 Data_Type","🗃 Describe_Data"])

                tab1.subheader("📋 Dados")
                with tab1:
                    st.dataframe(st.session_state.dados.head(10))

                tab2.subheader("📈 Tipo dos Dados")
                with tab2:
                    colunas_tipos = st.columns([2,3])
                    with colunas_tipos[0]:
                        st.markdown("##### Descrição sobre o Formato dos Dados")
                        string_markdown
                    with colunas_tipos[1]:
                        st.dataframe(pd.DataFrame({
                        "Coluna - DF": st.session_state.dados.dtypes.index,
                        "Tipo de Dado - Coluna": st.session_state.dados.dtypes.values
                    }))
                tab3.subheader("📊 Describe_Data")
                with tab3:
                    st.session_state.df_stats = st.session_state.dados.describe(include="all").T

                    # Adiciona coluna com a quantidade de nulos por coluna
                    st.session_state.df_stats["Nulos (qtd)"] = [st.session_state.dados[coluna_nulo].isnull().sum() for coluna_nulo in st.session_state.dados.columns]
                    st.session_state.df_stats["unique"] = [st.session_state.dados[coluna_unico].nunique() for coluna_unico in st.session_state.dados.columns]
                    # Exibe as estatísticas completas (incluindo os nulos)
                    st.write(st.session_state.df_stats)

            # Operações nas colunas
            Operacoes_Colunas = st.selectbox(
                "📑 Operações nas colunas",
                options=lista_funcoes_colunas,
                index=None,
                placeholder="Operações nas Colunas"
            )

            if Operacoes_Colunas:
                config = mapa_funcoes[Operacoes_Colunas]
                funcao = config["func"]
                entradas = config["inputs"]
                argumentos = []

                st.markdown("### Configurar Operação")

                for entrada in entradas:
                    if entrada == "coluna":
                        coluna = st.multiselect("Selecione a(s) coluna(s)", options=st.session_state.df_fun.columns, key="coluna")
                        argumentos.append(coluna)

                    elif entrada == "novacoluna":
                        st.markdown("### ⚙️ Inserir nova coluna?")
                        resposta_nova_coluna = st.selectbox("Deseja inserir nova coluna?", key="resposta_nova_coluna",placeholder="Selecione")  
                        if resposta_nova_coluna == "Sim":
                            valor = st.text_input("Digite o Nome da Nova Coluna", key="nome_nova_coluna")    
                            argumentos.append(valor)
                        else:
                            argumentos.append("Não")
                    
                    elif entrada == "Separador_Concatenar":
                        if len(coluna) > 1:
                            resposta_concatenar = st.selectbox("Deseja inserir un separador? ",["Sim", "Não"], key="resposta_concatenar", help=ajusa_separador,placeholder="Selecione")
                            if resposta_concatenar == "Sim":
                                valor = st.text_input("Digite o separador", key="nome_nova_coluna")    
                                argumentos.append(valor)
                            else:
                                argumentos.append("Não")
                        else:
                            st.write("Não é possivel concatenar, selecione mais de uma coluna")    

                    elif entrada == "valor":
                        tipo_valor = st.selectbox("Tipo de valor para preencher", ["Selecione...", "Número", "Texto"], key="tipo_valor")
                        if tipo_valor == "Texto":
                            valor = st.text_input("Digite o valor para preencher", key="valor_texto")
                            argumentos.append(valor)
                        elif tipo_valor == "Número":
                            valor = st.number_input("Digite o valor para preencher", key="valor_numero")
                            argumentos.append(valor)
                    
                    elif entrada == "condicao":
                        st.markdown("### ⚙️ Defina as condições de filtro para cada coluna selecionada")

                        filtros = {}

                        for col in coluna:
                            col_dtype = st.session_state.df_fun[col].dtype

                            with st.container():
                                cols = st.columns([2, 3])  # Operador / Valor

                                if pd.api.types.is_numeric_dtype(col_dtype):
                                    operador = cols[0].selectbox(
                                        f"Operador para `{col}`", 
                                        [">", "<", "==", ">=", "<=", "!=", "Entre"],
                                        key=f"operador_{col}"
                                    )

                                    if operador == "Entre":
                                        min_val = cols[1].number_input(f"Mínimo para `{col}`", key=f"min_{col}")
                                        max_val = cols[1].number_input(f"Máximo para `{col}`", key=f"max_{col}")
                                        filtros[col] = (operador, (min_val, max_val))
                                    else:
                                        val = cols[1].number_input(f"Valor para `{col}`", key=f"val_{col}")
                                        filtros[col] = (operador, val)

                                else:  # Tipo texto
                                    operador = cols[0].selectbox(
                                        f"Operador para `{col}`",
                                        ["igual a", "contém", "não contém", "começa com", "termina com"],
                                        key=f"operador_{col}"
                                    )
                                    val = cols[1].text_input(f"Texto para `{col}`", key=f"val_{col}")
                                    filtros[col] = (operador, val)

                        # Passa o dicionário diretamente para a função
                        argumentos.pop(0)
                        argumentos.append(filtros)

                    elif entrada == "operacao":
                        tipo_expressao = st.selectbox("Tipo da Operação", ["Linhas", "Colunas"], placeholder="Selecione", index=None)

                        if tipo_expressao == "Linhas":
                            expressao = st.selectbox("Operador", ["+", "-", "*", "/", "**", "//", "%"], placeholder="Selecione", index=None)
                            input_numerico = st.number_input("Digite o valor numericamente")

                            try:
                                operacao = eval(f"lambda x: x {expressao} {input_numerico}")
                                argumentos.append(operacao)
                                argumentos.append(tipo_expressao)
                            except Exception as e:
                                st.error(f"Expressão inválida. Exemplo: lambda x: x * 2. Erro: {e}")

                        elif tipo_expressao == "Colunas":
                            coluna_operador = st.selectbox("Coluna para operação", st.session_state.df_fun.columns)
                            expressao = st.selectbox("Operador", ["+", "-", "*", "/", "**", "//", "%"], placeholder="Selecione", index=None)
                            try:
                                operacao = [expressao,coluna_operador]
                                argumentos.append(operacao)
                                argumentos.append(tipo_expressao)
                            except Exception as e:
                                st.error(f"Expressão inválida. Exemplo: x['coluna1'] * x['coluna2']. Erro: {e}")

                        else:
                            st.error("Selecione um tipo de operação válido.")

                    elif entrada == "formato":
                        st.markdown("### 🗓️ Escolha o formato da data")
                        
                        formatos_comuns = {
                            "Dia/Mês/Ano (23/12/2024)": "%d/%m/%Y",
                            "Ano-Mês-Dia (2024-12-23)": "%Y-%m-%d",
                            "Mês/Dia/Ano (12/23/2024)": "%m/%d/%Y",
                            "Dia-Mês-Ano (23-12-2024)": "%d-%m-%Y",
                            "Data e hora (23/12/2024 14:30:00)": "%d/%m/%Y %H:%M:%S",
                            "Ano Mês Dia Hora Minuto Segundo (2024-12-23 00:00:00)":"%Y-%m-%d %H:%M:%S",
                            "ISO 8601 (2024-12-23T14:30:00)": "%Y-%m-%dT%H:%M:%S",
                            "Outro (preencher manualmente)": "manual"
                        }

                        escolha = st.selectbox("Selecione o formato da data", list(formatos_comuns.keys()))
                        
                        if formatos_comuns[escolha] == "manual":
                            formato = st.text_input("Digite o formato desejado (ex: %d/%m/%Y)")
                        else:
                            formato = formatos_comuns[escolha]
                        
                        argumentos.append(formato)

                if st.button("✅ Aplicar função"):
                    try:
                        # Aplica a função no DataFrame salvo
                        st.session_state.df_fun = funcao(st.session_state.df_fun, *argumentos)
                        st.success("✅ Função aplicada com sucesso!")
                        st.dataframe(st.session_state.df_fun.head())
                    except Exception as e:
                        st.error(f"❌ Erro ao aplicar a função: {e}")
                    
            # Operações no DataFrame completo
            Operacoes_Df = st.selectbox(
                "📑 Operações na Tabela",
                options=lista_funcoes_df,
                index=None,
                placeholder="Operações na Tabela"
            )

            if Operacoes_Df:
                config = mapa_funcoes_tabela[Operacoes_Df]
                funcao = config["func"]
                entradas = config["inputs"]
                argumentos = []

                for entrada in entradas:
                    if entrada == "coluna":
                        coluna = st.multiselect("Selecione a(s) coluna(s)", options=st.session_state.df_fun.columns, key="coluna")
                        argumentos.append(coluna)
                    
                    elif entrada == "novos_nomes_colunas":
                        st.markdown("### 🔠 Renomear Colunas")
                        
                        novo_nomes = {}  # Dicionário para armazenar {antigo: novo}
                        
                        for coluna_selecionada in coluna:  # 'colunas_selecionadas' deve ser sua lista de colunas
                            novo_nome = st.text_input(
                                f"Novo nome para `{coluna_selecionada}`:",
                                key=f"rename_{coluna_selecionada}",
                                help=f"Deixe em branco para manter o nome atual: {coluna_selecionada}"
                            )
                            if novo_nome:  # Só adiciona se foi digitado um novo nome
                                novo_nomes[coluna_selecionada] = novo_nome
                        
                        # Armazena o dicionário completo nos argumentos
                        argumentos.append(novo_nomes)
                        argumentos.pop(0)
                        #renomeacoes = {antigo: novo for antigo, novo in nomes.items() if antigo != novo}
                        #argumentos.append(renomeacoes)                    

                    
                    elif entrada == "agrupamentos":
                        st.markdown("### 🔠 Agrupamento/Group_by")
                        
                        colunas_agrupamento = st.multiselect("Selecione a(s) coluna(s)", options=coluna)
                        st.markdown(string_markdown)

                        nomes = {}  # Dicionário para armazenar os novos nomes
                        
                        for col in colunas_agrupamento:  # coluna deve ser a lista de colunas selecionadas
                            col_dtype = st.session_state.df_fun[col].dtype
                            with st.container():
                                cols = st.columns([3, 7])  # Ajuste as proporções conforme necessário
                                # Coluna 2: Input para o novo nome
                                novo_nome = cols[1].text_input(
                                    f"Novo nome para `{col}`",
                                    key=f"novo_nome_{col}",  # Chave única para cada input
                                    placeholder="Nome da Coluna" # Esconde o label padrão
                                )

                                if pd.api.types.is_numeric_dtype(col_dtype):
                                    # Coluna 1: Mostra o nome atual 
                                    operador = cols[0].selectbox(
                                            f"Operador para `{col}` - `{st.session_state.df_fun[col].dtype}`", 
                                            ["Somar", "Contar", "Contar Distinto", "Média"],
                                            key=f"operador_{col}"
                                        )
                                else:
                                    # Coluna 1: Mostra o nome atual 
                                    operador = cols[0].selectbox(
                                            f"Operador para `{col}` - `{st.session_state.df_fun[col].dtype}`", 
                                            ["Contar", "Contar Distinto"],
                                            key=f"operador_{col}"
                                        )
                                    
                            operador_mapeado = operadores_group_by[operador]

                            if novo_nome:  # Só adiciona se foi digitado um novo nome
                                nomes[novo_nome] = (col, operador_mapeado)
                            else:
                                nomes[col] = (col, operador_mapeado)                                                        

                            argumentos[0].remove(col)
                        argumentos.append(nomes)
                        
                if st.button("✅ Aplicar função"):
                    try:
                        st.session_state.df_fun = funcao(st.session_state.df_fun, *argumentos)
                        st.success("✅ Função aplicada com sucesso!")
                        st.dataframe(st.session_state.df_fun.head())
                    except Exception as e:
                        st.error(f"❌ Erro ao aplicar a função: {e}")

            st.markdown("### Dados - Alterados")                        
            with st.expander("📈 Dados Alterados"):
                tab_alterados1, tab_alterados2, tab_alterados3 = st.tabs(["📈 Dados", "🗃 Data_Type","🗃 Describe_Data"])

                tab_alterados1.subheader("📋 Dados")
                with tab_alterados1:
                    st.dataframe(st.session_state.df_fun.head(10))

                tab_alterados2.subheader("📈 Tipo dos Dados")
                with tab_alterados2:
                    colunas_tipos = st.columns([2,3])
                    with colunas_tipos[0]:
                        st.markdown("##### Descrição sobre o Formato dos Dados")
                        string_markdown
                    with colunas_tipos[1]:
                        st.dataframe(pd.DataFrame({
                        "Coluna - DF": st.session_state.df_fun.dtypes.index,
                        "Tipo de Dado - Coluna": st.session_state.df_fun.dtypes.values
                    }))
                tab_alterados3.subheader("📊 Describe_Data")
                with tab_alterados3:
                    st.session_state.df_stats_alter = st.session_state.df_fun.describe(include="all").T

                    st.session_state.df_stats_alter["Nulos (qtd)"] = [st.session_state.df_fun[coluna_nulo_alterado].isnull().sum() for coluna_nulo_alterado in st.session_state.df_fun.columns]
                    st.session_state.df_stats_alter["unique"] = [st.session_state.df_fun[coluna_unico_alterado].nunique() for coluna_unico_alterado in st.session_state.df_fun.columns]

                    # Exibe as estatísticas completas (incluindo os nulos)
                    st.write(st.session_state.df_stats_alter)
            with st.expander("📥 Download File"):
                if 'download_config' not in st.session_state:
                    st.session_state.download_config = False

                # Botão para ativar a configuração do download
                if st.button("✅ Configurar Arquivo de Download File"):
                    st.session_state.download_config = True
                
                if st.session_state.get("download_config", False):
                    gerar_arquivo_download(st.session_state.df_fun)

    col_aba_1, col_aba_2 = st.columns(2)

    with col_aba_1:
        if st.button("🏠 Voltar à página principal"):
            st.session_state['pagina'] = 'principal'
            st.rerun()

    with col_aba_2:
        if st.button("🔄 Recomeçar transformações"):
            if "dados" in st.session_state:
                st.session_state.df_fun = st.session_state.dados.copy()
                historico.limpar_historico()
                Historico()
                st.success("🔁 Dados reiniciados com sucesso.")
                st.rerun()
            else:
                st.warning("⚠️ Os dados ainda não foram carregados.")

# Processo 3
def processo_3():
    st.title("Hub - Relatórios Padrão")
    st.write("Bem vindo ao Hub de Relatórios Padrão! 👋")
    if "concluidos" not in st.session_state: 
        st.session_state["concluidos"] = []
    for i in st.session_state["concluidos"]:
        st.write(i)
    for rel in st.session_state.arquivos_atualizados:
        st.write(rel)

    Operacoes_Padrao = st.selectbox(
        "📑 Operações nas colunas",
        options=Funções_padrao,
        index=None,
        placeholder="Operações nas Colunas"
    )

    if Operacoes_Padrao:
        config = mapa_funcoes_tabela[Operacoes_Padrao]
        funcao = config["func"]
        funcao()

    if st.button("🏠 Voltar à página principal"):
        st.session_state['pagina'] = 'principal'
        st.rerun()

# Função principal
def main():
    # Caminho base
    caminho_raiz = os.path.dirname(os.path.abspath(__file__))

    # Inicializa estado
    if 'pagina' not in st.session_state:
        st.session_state['pagina'] = 'principal'
    if 'caminho_raiz' not in st.session_state:
        st.session_state['caminho_raiz'] = caminho_raiz
    if 'caminho_atual' not in st.session_state:
        st.session_state['caminho_atual'] = caminho_raiz

    # Roteamento de páginas
    if st.session_state['pagina'] == 'principal':
        pagina_principal()
    elif st.session_state['pagina'] == 'pastas':
        navegar_pastas()
    elif st.session_state['pagina'] == 'processo_2':
        processo_2()
    elif st.session_state['pagina'] == 'processo_3':
        processo_3()
        

if __name__ == "__main__":
    main()