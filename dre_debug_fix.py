#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DRE • Debug & Fix Pipeline

Objetivo:
1) Rodar o fluxo de rentabilidade usando mapeamentos (DB ou JSON), com merges blindados (1:1),
   normalização de chaves e logs completos de diagnósticos.
2) Gerar um Excel único com abas:
   - Rentabilidade_Armazem (base detalhada consolidada)
   - Consolidado_DRE (por grupos/tabelas)
   - De_Paras_Não_Encontrados (Razão/DRE)
   - De_Paras_Rateio_Não_Encontrados (Rateio/Volumes/Adequação/Insumos)
   - De_Paras_Duplicados (todos os mapeamentos com duplicidade)
   - Comparativo_Antigo (se arquivos antigos forem fornecidos)

Como usar:
- Ajuste as CONSTANTES abaixo para os caminhos de configuração e arquivos antigos (se quiser comparar).
- Execute:  python dre_debug_fix.py
- Saída:    DRE_Rentabilidade_FIX.xlsx

Notas:
- Se existir o módulo ServicoDePara com a função Carregar_Mapeamento_Banco(), o script tenta usar o banco.
  Caso contrário, cai no fallback via JSON (caminho_json_rentabilidade_armazem_dados.json).
- Os merges são feitos com proteção contra many-to-many. Duplicidades são logadas e deduplicadas antes do merge.
- Chaves textuais são normalizadas: strip, upper, remoção de acentos.
- Colunas códigos numéricos (ex.: Centro de Custo, Item, Conta) são coerced para string sem ".0".
"""

from __future__ import annotations
import os
import json
import math
import unicodedata
from typing import Dict, List, Optional, Tuple

import pandas as pd

# ==========================
# ===== CONFIGURAÇÃO =======
# ==========================
CONFIG_ARQUIVOS_JSON = "caminhos_dados_rentabilidade_armazem_arquivos.json"
CONFIG_DEPARA_JSON   = "caminho_json_rentabilidade_armazem_dados.json"

# Novo Excel de saída
OUTPUT_XLSX = "DRE_Rentabilidade_FIX.xlsx"

# Opcional: arquivos do pipeline antigo para comparativo (deixe vazio se não quiser)
OLD_Rentabilidade_Armazem = "Rentabilidade_Armazem.xlsx"  # aba Consolidado_Rentabilidade
OLD_Consolidado_DRE       = "Consolidado - DRE.xlsx"
OLD_DeParas               = "De Paras Não Encontrados.xlsx"
OLD_DeParas_Rateio        = "De Paras Rateio Não Encontrados.xlsx"

# Filtro padrão de faturamento (ajuste conforme necessário)
FAT_EMPRESAS = {"FARMA", "FARMA DIST"}
FAT_VERSAO   = {"REAL"}
FAT_RECEITA  = {"SERVIÇOS"}
FAT_ANOS     = None  # ex.: {2025}; None = não força filtro por ano além do que vier do arquivo
FAT_LIQ_MULT = 0.9075

# ==========================
# ===== UTILITÁRIOS  =======
# ==========================

def _strip_accents(s: str) -> str:
    if not isinstance(s, str):
        s = "" if s is None else str(s)
    s = s.replace("\u00a0", " ")  # NBSP → space
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])


def norm_txt(x) -> str:
    """Normaliza textos para merges: strip, upper, sem acento."""
    if x is None:
        return ""
    s = str(x).strip()
    s = _strip_accents(s).upper()
    return s


def norm_code(x) -> str:
    """Normaliza códigos numéricos: vira string limpa, sem .0, sem espaços."""
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s


def coerce_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def load_json(path: str) -> dict:
    if not os.path.exists(path):
        raise FileNotFoundError(f"JSON não encontrado: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# =======================================
# ===== CARREGADORES: MAPEAMENTOS =======
# =======================================

def try_load_depara_from_db() -> Optional[Dict[str, pd.DataFrame]]:
    """Tenta usar ServicoDePara.Carregar_Mapeamento_Banco().
    Deve retornar dict com chaves: 'itens', 'centro_custo', 'filial', 'contas'.
    Se não existir/der erro, retorna None.
    """
    try:
        from ServicoDePara import Carregar_Mapeamento_Banco  # type: ignore
        maps = Carregar_Mapeamento_Banco()
        # Esperado: dict de DataFrames
        if not isinstance(maps, dict):
            return None
        out: Dict[str, pd.DataFrame] = {}
        # Normaliza nomes de chaves comuns
        key_map = {
            'DRE_De_Para_Item_Conta': 'itens',
            'DRE_De_Para_Centro_Custo': 'centro_custo',
            'DRE_De_Para_Filial': 'filial',
            'DRE_De_Para_Contas_Contabeis': 'contas',
        }
        for k, v in maps.items():
            name = key_map.get(k, norm_txt(k).lower())
            out[name] = v.copy()
        return out
    except Exception:
        return None


def load_depara_from_json(config_depara_json: str) -> Dict[str, pd.DataFrame]:
    """Carrega mapeamentos do JSON (estrutura com 'De_Para' → sheet_name → {columns, data})."""
    cfg = load_json(config_depara_json)
    if "De_Para" not in cfg or "sheet_name" not in cfg["De_Para"]:
        raise ValueError("JSON de de-para inválido: não há 'De_Para.sheet_name'")

    sheets = cfg["De_Para"]["sheet_name"]
    dfs: Dict[str, pd.DataFrame] = {}

    def make_df(obj: dict) -> pd.DataFrame:
        cols = obj.get("columns", [])
        data = obj.get("data", [])
        return pd.DataFrame(data, columns=cols)

    # Mapas esperados
    if "DRE_De_Para_Item_Conta" in sheets:
        dfs["itens"] = make_df(sheets["DRE_De_Para_Item_Conta"]).copy()
    if "DRE_De_Para_Centro_Custo" in sheets:
        dfs["centro_custo"] = make_df(sheets["DRE_De_Para_Centro_Custo"]).copy()
    if "DRE_De_Para_Filial" in sheets:
        dfs["filial"] = make_df(sheets["DRE_De_Para_Filial"]).copy()
    if "DRE_De_Para_Contas_Contabeis" in sheets:
        dfs["contas"] = make_df(sheets["DRE_De_Para_Contas_Contabeis"]).copy()

    return dfs


def load_all_mappings() -> Tuple[Dict[str, pd.DataFrame], List[pd.DataFrame]]:
    """Carrega mapeamentos (DB se disponível; senão JSON) e normaliza colunas-chave.
    Retorna (maps, logs_dup), onde logs_dup é lista de DFs com duplicidades.
    """
    logs_dup: List[pd.DataFrame] = []

    maps = try_load_depara_from_db()
    if maps is None:
        maps = load_depara_from_json(CONFIG_DEPARA_JSON)

    # Normalizações de colunas comuns:
    if "itens" in maps:
        df = maps["itens"].copy()
        # Esperado: [Item, Nome, Sigla]
        if "Item" in df.columns:
            df["Item_norm"] = df["Item"].map(norm_code)
        else:
            df["Item_norm"] = ""
        if "Sigla" not in df.columns:
            df["Sigla"] = "Desconhecido"
        # Dedup por Item_norm
        dup = df[df.duplicated(["Item_norm"], keep=False)].copy()
        if not dup.empty:
            dup.insert(0, "_MAPA_", "Item_Conta")
            logs_dup.append(dup)
        df = df.drop_duplicates(["Item_norm"], keep="first")
        maps["itens"] = df

    if "centro_custo" in maps:
        df = maps["centro_custo"].copy()
        # Esperado: [Centro de Custo, Centro Custo, Tipo CC]
        key = None
        for c in ["Centro de Custo", "Centro_de_Custo", "Centro", "CC", "Centro Custo"]:
            if c in df.columns:
                key = c
                break
        if key is None:
            df["Centro_de_Custo_norm"] = ""
        else:
            df["Centro_de_Custo_norm"] = df[key].map(norm_code)
        if "Tipo CC" not in df.columns:
            df["Tipo CC"] = "Oper"
        dup = df[df.duplicated(["Centro_de_Custo_norm"], keep=False)].copy()
        if not dup.empty:
            dup.insert(0, "_MAPA_", "Centro_Custo")
            logs_dup.append(dup)
        df = df.drop_duplicates(["Centro_de_Custo_norm"], keep="first")
        maps["centro_custo"] = df

    if "filial" in maps:
        df = maps["filial"].copy()
        # Esperado: [Filial, Filial UF]
        if "Filial" in df.columns:
            df["Filial_norm"] = df["Filial"].map(norm_txt)
        else:
            df["Filial_norm"] = ""
        if "Filial UF" not in df.columns:
            df["Filial UF"] = "SP"
        # Tratar ITAJAÍ vs ITAJAI
        df["Filial_norm"] = df["Filial_norm"].str.replace("ITAJAI", "ITAJAÍ")
        dup = df[df.duplicated(["Filial_norm"], keep=False)].copy()
        if not dup.empty:
            dup.insert(0, "_MAPA_", "Filial")
            logs_dup.append(dup)
        df = df.drop_duplicates(["Filial_norm"], keep="first")
        maps["filial"] = df

    if "contas" in maps:
        df = maps["contas"].copy()
        # Esperado: [conta, descrição resumida, Grupo, Grupo Financeiro] (e possivelmente "Concat Razão")
        key = None
        for c in ["conta", "Conta", "CONTA"]:
            if c in df.columns:
                key = c
                break
        if key is None:
            df["conta_norm"] = ""
        else:
            df["conta_norm"] = df[key].map(norm_code)
        if "Grupo" not in df.columns:
            df["Grupo"] = df.get("descrição resumida", df.get("descricao", "OUTROS")).astype(str)
        if "Grupo Financeiro" not in df.columns:
            df["Grupo Financeiro"] = "Não Financeiro"
        dup = df[df.duplicated(["conta_norm"], keep=False)].copy()
        if not dup.empty:
            dup.insert(0, "_MAPA_", "Contas")
            logs_dup.append(dup)
        df = df.drop_duplicates(["conta_norm"], keep="first")
        maps["contas"] = df

    return maps, logs_dup


# ===================================
# ===== CARREGADORES: DADOS    ======
# ===================================

def load_cfg_arquivos() -> dict:
    return load_json(CONFIG_ARQUIVOS_JSON)


def load_excel(path: str, sheet: Optional[str|int|List[str]] = None, header: Optional[int|List[int]] = 0) -> pd.DataFrame|Dict[str, pd.DataFrame]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    xl = pd.ExcelFile(path)
    return pd.read_excel(xl, sheet_name=sheet, header=header)


def load_dre_razao(cfg: dict) -> pd.DataFrame:
    dre = cfg.get("DRE", {})
    path = dre.get("path")
    sheets = dre.get("sheet_name", [])
    header = dre.get("header", 0)
    if not path:
        raise ValueError("Config DRE.path ausente")
    if not sheets:
        raise ValueError("Config DRE.sheet_name ausente")
    dfs = []
    data = load_excel(path, sheet=sheets, header=header)
    if isinstance(data, dict):
        for name, df in data.items():
            dfs.append(df.copy())
    else:
        dfs.append(data.copy())
    base = pd.concat(dfs, ignore_index=True)

    # Normalizações comuns
    base.columns = [str(c).strip() for c in base.columns]
    # Campos prováveis
    for col in ["Conta", "Título Conta", "Data", "Descrição", "Filial", "Centro de Custo", "Item", "saldo"]:
        if col not in base.columns:
            # cria se faltar
            base[col] = None
    base["Conta_norm"] = base["Conta"].map(norm_code)
    base["Centro_de_Custo_norm"] = base["Centro de Custo"].map(norm_code)
    base["Item_norm"] = base["Item"].map(norm_code)
    base["Filial_norm"] = base["Filial"].map(norm_txt)
    base["Data"] = pd.to_datetime(base["Data"], errors="coerce", dayfirst=True)
    base["Ano"] = base["Data"].dt.year
    base["saldo_num"] = coerce_numeric(base["saldo"]) if "saldo" in base.columns else 0.0
    return base


def load_faturamento(cfg: dict) -> pd.DataFrame:
    fat_cfg = cfg.get("Faturamento", {})
    path = fat_cfg.get("path")
    if not path:
        return pd.DataFrame()

    sheet = fat_cfg.get("sheet_name", 0)
    header = fat_cfg.get("header", 0)
    df = load_excel(path, sheet=sheet, header=header)
    if isinstance(df, dict):
        # se veio dict por multiheader, pega primeira aba
        df = list(df.values())[0]
    df.columns = [str(c).strip() for c in df.columns]

    # Normaliza campos usuais
    ren = {
        "EMPRESA": "EMPRESA",
        "FILIAL": "FILIAL",
        "CLIENTE": "CLIENTE",
        "RECEITA": "RECEITA",
        "VERSÃO": "VERSAO",
        "MÊS": "MES",
        "ANO": "ANO",
        "TIPO": "TIPO",
        "VALOR R$": "VALOR",
    }
    for k, v in ren.items():
        if k in df.columns:
            df[v] = df[k]
        elif v not in df.columns:
            df[v] = None

    df["EMPRESA"] = df["EMPRESA"].astype(str).str.upper().str.strip()
    df["RECEITA"] = df["RECEITA"].astype(str).str.upper().str.strip()
    df["VERSAO"] = df["VERSAO"].astype(str).str.upper().str.strip()
    df["ANO"] = pd.to_numeric(df["ANO"], errors="coerce").astype("Int64")
    df["VALOR"] = coerce_numeric(df["VALOR"])

    # Filtros padrão
    if FAT_EMPRESAS:
        df = df[df["EMPRESA"].isin(FAT_EMPRESAS)].copy()
    if FAT_VERSAO:
        df = df[df["VERSAO"].isin(FAT_VERSAO)].copy()
    if FAT_RECEITA:
        df = df[df["RECEITA"].isin(FAT_RECEITA)].copy()
    if FAT_ANOS:
        df = df[df["ANO"].isin(FAT_ANOS)].copy()

    # Líquido
    df["VALOR_LIQ"] = df["VALOR"] * FAT_LIQ_MULT
    return df


# ===================================
# ===== MERGE SEGURO & LOGS   =======
# ===================================

def safe_merge(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_on: List[str],
    right_on: List[str],
    how: str = "left",
    suffixes: Tuple[str, str] = ("", "_m"),
    mapa_nome: str = "",
    cols_keep: Optional[List[str]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Merge 1:m (left) com dedupe no right para virar 1:1. Loga duplicidades (right)
    e não encontrados (left com chave não-nula e sem match).
    Retorna: (merged, dup_right, not_found)
    """
    r = right.copy()
    # Garante unicidade no RIGHT
    dup_mask = r.duplicated(right_on, keep=False)
    dup_right = r.loc[dup_mask].copy()
    if not dup_right.empty:
        dup_right.insert(0, "_MAPA_", mapa_nome)
    # Dedup conservador (primeiro)
    r = r.drop_duplicates(right_on, keep="first")

    m = left.merge(r if cols_keep is None else r[right_on + cols_keep],
                   how=how, left_on=left_on, right_on=right_on, suffixes=suffixes)

    # Not found: chave não nula no left e colunas do right ausentes
    left_key = left[left_on].astype(str).agg("|".join, axis=1)
    right_key = r[right_on].astype(str).agg("|".join, axis=1)
    m_key = m[left_on].astype(str).agg("|".join, axis=1)

    found_keys = set(right_key)
    nf_mask = m_key.apply(lambda k: (k not in ("|".join([""]*len(left_on))) and k not in found_keys))
    not_found = m.loc[nf_mask].copy()
    if not not_found.empty:
        not_found.insert(0, "_MAPA_", mapa_nome)

    return m, dup_right, not_found


# ===================================
# ===== PROCESSAMENTO PRINCIPAL =====
# ===================================

def processar() -> None:
    cfg = load_cfg_arquivos()
    razao = load_dre_razao(cfg)
    fatur = load_faturamento(cfg)

    maps, logs_dup_list = load_all_mappings()
    itens = maps.get("itens", pd.DataFrame())
    ccs   = maps.get("centro_custo", pd.DataFrame())
    fil   = maps.get("filial", pd.DataFrame())
    ctas  = maps.get("contas", pd.DataFrame())

    # ===== Merges protegidos =====
    # 1) Centro de Custo → Tipo CC
    keep_cc = [c for c in ["Centro Custo", "Tipo CC"] if c in ccs.columns]
    razao, dup_cc, nf_cc = safe_merge(
        razao, ccs, ["Centro_de_Custo_norm"], ["Centro_de_Custo_norm"], mapa_nome="Centro_Custo", cols_keep=keep_cc
    )

    # 2) Item → Sigla
    keep_it = [c for c in ["Nome", "Sigla"] if c in itens.columns]
    razao, dup_it, nf_it = safe_merge(
        razao, itens, ["Item_norm"], ["Item_norm"], mapa_nome="Item_Conta", cols_keep=keep_it
    )

    # 3) Filial → UF
    keep_fi = [c for c in ["Filial", "Filial UF"] if c in fil.columns]
    razao, dup_fi, nf_fi = safe_merge(
        razao, fil, ["Filial_norm"], ["Filial_norm"], mapa_nome="Filial", cols_keep=keep_fi
    )

    # 4) Conta → Grupo/Grupo Financeiro
    keep_ct = [c for c in ["descrição resumida", "Grupo", "Grupo Financeiro"] if c in ctas.columns]
    razao, dup_ct, nf_ct = safe_merge(
        razao, ctas, ["Conta_norm"], ["conta_norm"], mapa_nome="Contas", cols_keep=keep_ct
    )

    # Consolida logs
    logs_dup = pd.concat([df for df in [dup_cc, dup_it, dup_fi, dup_ct] if df is not None and not df.empty], ignore_index=True) if any([not x.empty for x in [dup_cc, dup_it, dup_fi, dup_ct]]) else pd.DataFrame()
    nas_depara_razao = pd.concat([df for df in [nf_cc, nf_it, nf_fi, nf_ct] if df is not None and not df.empty], ignore_index=True) if any([not x.empty for x in [nf_cc, nf_it, nf_fi, nf_ct]]) else pd.DataFrame()

    # ===== Classificação Tabela (regra objetiva e simples) =====
    # Ajuste aqui conforme a régua desejada
    def class_tabela(row) -> str:
        grupo_fin = str(row.get("Grupo Financeiro", "")).upper()
        tipo_cc   = str(row.get("Tipo CC", "")).upper()
        sigla     = str(row.get("Sigla", "")).upper()
        desc_res  = str(row.get("descrição resumida", "")).upper()

        if "FINANCEIR" in grupo_fin or "FINANCEIR" in desc_res:
            return "Custos Financeiros"
        if "IMPOSTO" in desc_res or desc_res in {"PIS", "COFINS", "ICMS", "ISS"}:
            return "Outros Impostos"
        if "EMBALAG" in desc_res or "ADEQUA" in desc_res:
            return "Embalagem/Adequação"
        if tipo_cc == "ADM":
            return "Overhead"
        # Operacional
        if sigla in {"DIRETO", "DIRETA", "FARMA", "FARMA DIRETO"}:
            return "Custos Operacionais Diretos"
        return "Custos Operacionais Indiretos"

    razao["Tabela"] = razao.apply(class_tabela, axis=1)

    # ===== Consolidado DRE (por Tabela e Ano) =====
    cons_dre = (
        razao.groupby(["Ano", "Tabela"], dropna=False)["saldo_num"].sum().reset_index()
        .sort_values(["Ano", "Tabela"]) )

    # ===== Faturamento líquido =====
    if not fatur.empty:
        fat_ano = fatur.groupby(["ANO"], dropna=False)["VALOR_LIQ"].sum().reset_index()
        fat_ano["Tabela"] = "Faturamento"
        fat_ano.rename(columns={"ANO": "Ano", "VALOR_LIQ": "saldo_num"}, inplace=True)
        cons_dre = pd.concat([cons_dre, fat_ano], ignore_index=True)

    # ===== Consolidado_Rentabilidade (pivot Tabela x Ano) =====
    cons_pivot = cons_dre.pivot_table(index=["Tabela"], columns=["Ano"], values="saldo_num", aggfunc="sum", fill_value=0)
    cons_pivot.reset_index(inplace=True)

    # ===== Rateio/Volumes/Adequação/Insumos não encontrados (placeholder) =====
    nas_depara_rateio = pd.DataFrame(columns=["_MAPA_", "CHAVE", "MOTIVO"])  # se necessário, pode-se ampliar

    # ===== Comparativo com antigo (se arquivos disponíveis) =====
    comparativo_antigo = pd.DataFrame()
    try:
        if os.path.exists(OLD_Rentabilidade_Armazem):
            old = pd.read_excel(OLD_Rentabilidade_Armazem, sheet_name="Consolidado_Rentabilidade")
            old = old.copy()
            # Tenta reconhecer colunas
            if "Tabela" not in old.columns:
                # heurística
                if "GRUPO" in old.columns:
                    old.rename(columns={"GRUPO": "Tabela"}, inplace=True)
            # Somatório por Tabela
            old_sum = old.copy()
            numcol = None
            for c in ["saldo_num", "saldo", "VALOR", "VALOR_LIQ"]:
                if c in old_sum.columns:
                    numcol = c
                    break
            if numcol is None:
                # tenta converter tudo numérico e somar
                old_sum["VALOR"] = coerce_numeric(old_sum.select_dtypes(include=["number"]).sum(axis=1))
                numcol = "VALOR"
            old_sum = old_sum.groupby("Tabela", dropna=False)[numcol].sum().reset_index().rename(columns={numcol: "old_val"})

            new_sum = cons_dre.groupby("Tabela", dropna=False)["saldo_num"].sum().reset_index().rename(columns={"saldo_num": "new_val"})

            comparativo_antigo = new_sum.merge(old_sum, on="Tabela", how="outer")
            comparativo_antigo["diff"] = comparativo_antigo["new_val"].fillna(0) - comparativo_antigo["old_val"].fillna(0)
    except Exception as e:
        comparativo_antigo = pd.DataFrame({"erro": [str(e)]})

    # ===== Escrita do Excel =====
    with pd.ExcelWriter(OUTPUT_XLSX, engine="xlsxwriter") as wr:
        # Base detalhada (pode ser pesada; amostra de colunas relevantes)
        cols_base = [c for c in [
            "Data", "Ano", "Filial", "Filial UF", "Centro de Custo", "Tipo CC", "Item", "Sigla",
            "Conta", "descrição resumida", "Grupo", "Grupo Financeiro", "Tabela", "saldo", "saldo_num"
        ] if c in razao.columns]
        razao[cols_base].to_excel(wr, sheet_name="Rentabilidade_Armazem", index=False)

        cons_dre.to_excel(wr, sheet_name="Consolidado_DRE", index=False)
        cons_pivot.to_excel(wr, sheet_name="Consolidado_Pivot", index=False)

        if not logs_dup.empty:
            logs_dup.to_excel(wr, sheet_name="De_Paras_Duplicados", index=False)
        else:
            pd.DataFrame({"info": ["Sem duplicidades em de-para"]}).to_excel(wr, sheet_name="De_Paras_Duplicados", index=False)

        if nas_depara_razao is not None and not nas_depara_razao.empty:
            nas_depara_razao.to_excel(wr, sheet_name="De_Paras_Não_Encontrados", index=False)
        else:
            pd.DataFrame({"info": ["Sem não encontrados no Razão"]}).to_excel(wr, sheet_name="De_Paras_Não_Encontrados", index=False)

        if nas_depara_rateio is not None:
            nas_depara_rateio.to_excel(wr, sheet_name="De_Paras_Rateio_Não_Encontrados", index=False)

        if comparativo_antigo is not None and not comparativo_antigo.empty:
            comparativo_antigo.to_excel(wr, sheet_name="Comparativo_Antigo", index=False)

    print(f"OK • Arquivo gerado: {OUTPUT_XLSX}")


if __name__ == "__main__":
    processar()
