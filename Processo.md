Com certeza! Vamos mergulhar fundo no funcionamento do seu sistema **T-Core Control**. Vou explicar o processo como uma história, dividida em capítulos, detalhando desde a origem dos dados até o relatório final.

O processo é um fluxo de **ETL** (Extract, Transform, Load) clássico, orquestrado pelo Python (Flask + Pandas).

---

### 🏗️ Fase 0: A Fundação (Configurações e Banco de Dados)

Antes de processar qualquer arquivo, o sistema se prepara.

1.  **Carregamento de Configurações (`Config.py`):**
    * O sistema define onde estão os arquivos Excel no seu computador (caminho `C:\\Projetos\\DRE\\...`).
    * Define regras rígidas, como quais colunas ler e em quais abas.

2.  **Conexão com Banco de Dados (`Db/Connection.py`):**
    * O sistema conecta ao **PostgreSQL**.
    * **Cache Inteligente:** Ele baixa 10 tabelas de "De-Para" (mapeamentos) e as guarda na memória. Se você rodar o processo duas vezes seguidas, ele não vai ao banco de novo (o cache dura 1 hora).
    * **Tabelas Importantes:**
        * `Tb_DRE_De_Para_Centro_Custo`: Traduz códigos de centro de custo.
        * `Tb_DRE_De_Para_Contas_Contabeis`: A tabela mais vital, que diz como classificar cada conta contábil.
        * `Tb_Volumes_De_Para_Abreviacao`: Traduz nomes de clientes para grupos comerciais.

---

### 📦 Fase 1: O Processamento dos "Satélites" (Rateio)

Esta fase ocorre no arquivo `Services/DRE/ServicoRelatoriosRateio.py`. O sistema processa 5 arquivos auxiliares que não são o DRE contábil, mas compõem o resultado.

#### 1. Volumes (Saída)
* **Arquivo:** `Volumes - Base.xlsx`
* **Lógica:**
    1.  Lê o Excel e padroniza nomes de colunas (ex: "DATAFIMPEDIDO").
    2.  **Limpeza de Data:** Tenta converter datas. Se falhar, pega apenas a parte do texto antes do espaço.
    3.  **Merge (De-Para):** Cruza o nome do `CLIENTE` com a tabela `Volumes_De_Para_Abreviacao` para descobrir o **Grupo**.
    4.  **Regra Hardcoded:** Se a Filial for "ITJ", o código força a troca para "SC".
    5.  **Resultado:** Soma tudo agrupando por Mês, Ano, Filial, Area, Grupo e Item.

#### 2. Adequação (Serviços Extras)
* **Arquivo:** `Quantidade - Adequação.xlsx`
* **Lógica:**
    1.  Lê colunas como "Qtde Real" e "Nome Servico".
    2.  **Merge (De-Para):** Igual ao Volumes, usa `Volumes_De_Para_Abreviacao` para traduzir `Cliente` -> `Grupo`.
    3.  **Resultado:** Define a "Tabela" como "Relatório de Adequação" e agrupa as somas.

#### 3. Insumos (Custos de Materiais)
* **Arquivo:** `Insumos.xlsx`
* **Lógica:**
    1.  Extrai o **Ano** pegando os 4 primeiros dígitos da coluna `ID`.
    2.  **Merge 1 (Filial):** Usa a tabela `Embalagens_De_Para_Clientes` cruzando com `NOMECLI` para descobrir a **Filial UF**.
    3.  **Merge 2 (Grupo):** Usa a tabela `Volumes_De_Para_Abreviacao` cruzando com `Depositante` para descobrir o **Grupo**.
    4.  **Regra de Negócio (Matemática):** O valor do custo (`saldo`) é multiplicado por **0.9075**. Isso geralmente representa um desconto de impostos (como PIS/COFINS) ou margem interna para chegar ao custo líquido.

#### 4. Faturamento (Receita)
* **Arquivo:** `Faturamento 2025.xlsx` (Aba: "base")
* **Filtros Rígidos:** O código só aceita linhas onde:
    * Empresa é "FARMA" ou "FARMA DIST".
    * Ano é 2025.
    * Versão é "Real".
    * Receita é "Serviços".
* **Regra de Negócio (Matemática):** Aplica o mesmo fator de **0.9075** sobre o valor.
* **Merge (Filial):** Cruza o nome da Filial (ex: "Barueri") com `DRE_De_Para_Filial` para pegar a sigla (ex: "SP").

#### 5. Ocupação de Armazém (Pallets)
* **Arquivo:** `Acompanhamento Pallets 2025.xlsx`
* **Complexidade:** É o arquivo mais difícil. O Excel original é uma "tabela dinâmica" (pivotada) com datas nas colunas.
* **Lógica de "Unpivot":**
    1.  O código lê as abas SP, SC, RJ, GO.
    2.  Identifica colunas de Clientes vs. Colunas de Totais.
    3.  Transforma as colunas de datas em linhas (empilha os dados), de modo que "Janeiro", "Fevereiro" virem valores na coluna "Mês".
    4.  **Merge (Grupo):** Usa a tabela `De_Para_Grupos_Ocupacao` cruzando **Cliente + Filial** para achar o **Grupo**.

---

### 📒 Fase 2: O Coração (DRE / Razão Contábil)

Esta fase ocorre em `Services/DRE/ServicoRelatoriosDRE.py`. Aqui trabalhamos com o **Razão Contábil** (`Resultado DRE Mensal 2025_v2.xlsx`).

A lógica aqui é **Destrutiva**: O sistema carrega o arquivo inteiro e vai "recortando" pedaços dele. O que sobra no final é o custo operacional "puro".

#### Passo 1: Enriquecimento (Merges)
Antes de recortar, ele adiciona inteligência ao arquivo cru:
1.  **Centro de Custo:** Adiciona descrição via `DRE_De_Para_Centro_Custo`.
2.  **Item:** Adiciona nome do item via `DRE_De_Para_Item_Conta`.
3.  **Filial:** Adiciona UF via `DRE_De_Para_Filial`.
4.  **Contas Contábeis (O Grande Merge):**
    * *Tentativa 1:* Tenta casar `Conta` + `TipoCC` com a tabela do banco.
    * *Tentativa 2 (Fallback):* Se falhar, tenta casar apenas pelo número da `Conta`. Isso garante que contas novas ou cadastradas incorretamente ainda tenham chance de serem classificadas.

#### Passo 2: Recortes Específicos (`Embalagem_Adequa`)
O sistema começa a retirar dados do montante principal e separar em "caixinhas":
* **Folha Adequação:** Se Item for '10110' e Grupo 'PESSOAL OPER'.
* **Embalagens:** Se Título for 'MATERIAL DE EMBALAGEM'.
* **Custos Financeiros/Depreciação:** Baseado no `grupo_financeiro`.
    * *Detalhe:* Depreciação sofre um De-Para extra para corrigir a Filial.
* **ISS:** Se Grupo for 'ISS'.
    * *Regra Hardcoded:* Mapeia itens específicos (ex: '10802') para UFs específicas (ex: 'GO'), ignorando o que veio no Excel original.
* **Outros Impostos:** PIS, COFINS, ICMS.
* **Taxas:** Divide em "Operacionais - Taxas" ou "Indiretos - Taxas" dependendo se o Centro de Custo é Armazenagem ou não.

> *Nota:* Tudo que foi identificado aqui é **removido** da lista principal (`self.Razao_Farma_Consolidado`).

#### Passo 3: Recorte de Overhead (`Overhead`)
Do que sobrou:
* **Overhead Não Operacional:** Tudo que no De-Para de Contas tinha o `tipo_cc` diferente de "Oper".
* **Indenizações:** Conta específica `60301020108`.

#### Passo 4: Classificação Direto vs. Indireto
O que sobrou é Custo Operacional. O sistema marca:
* **Farma Direto:** Se a sigla do grupo (vinda do De-Para) existe E o centro de custo é "Operação Armazenagem".
* **Farma Indireto:** Se a sigla é "Desconhecido" (não tem cliente específico atrelado).

#### Passo 5: Alocação Final (`custos_alocados`)
Agora ele dá o nome final para as linhas restantes baseadas na classificação acima:
* **Folha Razão:** Pessoal Operacional Direto.
* **Rateio Indiretos:** Pessoal Operacional Indireto.
* **Temporários:** Terceiros Operacionais ou conta `60301020209`.
* **Custos Operacionais:** Informática, Armazenagem e Outros.

---

### 🚀 Fase 3: Consolidação Final

1.  **Juntar Tudo:** O método `consolidado()` pega todos os DataFrames gerados na Fase 1 (Rateio) e todos os recortes da Fase 2 (DRE).
2.  **Empilhamento:** Usa `pd.concat` para criar uma tabela gigante única.
3.  **Tratamento de Nulos:** Substitui qualquer vazio por "N/A" para não quebrar o Excel.
4.  **Relatórios de Erro:** O sistema gera duas abas extras:
    * `De_Paras_Não_Encontrados`: Mostra o que veio no DRE mas não tinha no banco de dados.
    * `De_Paras_Rateio_Não_Encontrados`: Mostra clientes/insumos dos arquivos auxiliares que não tinham cadastro.

### Resultado Final

O arquivo Excel gerado (`DRE_Rentabilidade_UUID.xlsx`) terá:
1.  **Rentabilidade_Armazem:** A aba principal com todos os números consolidados.
2.  **Consolidado_DRE:** Uma cópia do Razão tratado (para conferência).
3.  **Abas de Erro:** Para a controladoria saber o que precisa cadastrar no banco.

Essa arquitetura é muito robusta porque separa a **lógica de negócio** (Python) dos **dados de configuração** (Banco de Dados), permitindo que você altere regras contábeis apenas mudando o banco, sem precisar reprogramar o Python.