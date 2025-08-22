from __future__ import annotations
from pathlib import Path
from typing import Dict, Iterable, List
import sys
import pandas as pd

# =========================
# CONFIGURAÇÕES GERAIS
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR  = BASE_DIR / "raw"         # raw/2018, raw/2019, ...
DIM_DIR  = BASE_DIR / "data" / "dimensions"
OUT_DIR  = BASE_DIR / "data"

# Nome do arquivo final
FACT_OUT = OUT_DIR / "fact_dataset.parquet"
# Também salvar em feather (rápido para leitura)?
SAVE_FEATHER = True
FACT_OUT_FEATHER = OUT_DIR / "fact_dataset.feather"

# Quais dimensões aplicar (código na fato -> label)
# chave: nome da coluna no dataset fato | valor: nome da dimensão (arquivo Dim_<dim>.csv)
DIMENSIONS_TO_APPLY = {
    "REGLT": "REGLT",
    "ZELT":  "ZELT",
    # adicione outras se existirem na fato: "AGED": "AGED", ...
}

# =========================
# UTILITÁRIOS
# =========================
def log(msg: str) -> None:
    print(f"[etl] {msg}")

def list_year_dirs(raw_dir: Path) -> List[Path]:
    """Retorna subpastas de ano em raw/ (ex.: raw/2018, raw/2019)."""
    years = []
    for p in sorted(raw_dir.iterdir()):
        if p.is_dir() and p.name.isdigit():
            years.append(p)
    return years

def list_parquets(folder: Path) -> List[Path]:
    """Lista arquivos .parquet dentro de uma pasta."""
    return sorted(folder.glob("*.parquet"))

def safe_concat(dfs: List[pd.DataFrame]) -> pd.DataFrame:
    """Concatena dataframes de forma segura."""
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True, sort=False)

# =========================
# DIMENSÕES (i18n)
# =========================
def load_dimension(dim_name: str) -> pd.DataFrame:
    """
    Lê Dim_<dim>.csv (sep=';') e retorna DataFrame com:
      COD_<dim>, Desc_lbl_fr, Desc_lbl_en, Desc_lbl_pt
    """
    path = DIM_DIR / f"Dim_{dim_name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Dimensão não encontrada: {path}")
    df = pd.read_csv(path, sep=";", encoding="utf-8-sig", dtype=str).fillna("")
    return df

def make_label_map(dim_name: str, lang: str = "fr") -> Dict[str, str]:
    """
    Monta dict code->label no idioma escolhido.
    Fallback: se EN/PT vazio, usa FR.
    """
    df = load_dimension(dim_name)
    code_col = f"COD_{dim_name}"
    label_col = {"fr":"Desc_lbl_fr", "en":"Desc_lbl_en", "pt":"Desc_lbl_pt"}[lang]
    labels = df.apply(lambda r: r[label_col] if r[label_col].strip() else r["Desc_lbl_fr"], axis=1)
    return dict(zip(df[code_col].astype(str), labels.astype(str)))

def apply_dimensions(df: pd.DataFrame, dims: Dict[str, str], lang: str) -> pd.DataFrame:
    """
    Para cada coluna (ex.: 'REGLT') aplica label usando dimensão Dim_<nome>.
    Cria coluna nova: <col>_lbl (ex.: REGLT_lbl).
    """
    out = df.copy()
    for col, dim in dims.items():
        if col not in out.columns:
            log(f"coluna '{col}' não encontrada na fato; pulando Dim_{dim}")
            continue
        m = make_label_map(dim, lang=lang)
        out[f"{col}_lbl"] = out[col].astype(str).map(m).fillna(out[col].astype(str))
    return out

# =========================
# ETL POR ARQUIVO / ANO
# =========================
def etl_one_parquet(path: Path) -> pd.DataFrame:
    """
    Lê um parquet e faz transformações específicas (se houver).
    Aqui é o lugar para normalizar tipos, renomear colunas, etc.
    """
    log(f"lendo {path.name}")
    df = pd.read_parquet(path)
    # Exemplo: garantir que chaves geográficas fiquem como string
    for key in DIMENSIONS_TO_APPLY.keys():
        if key in df.columns:
            df[key] = df[key].astype(str)
    return df

def etl_one_year(year_dir: Path) -> pd.DataFrame:
    """Lê e concatena todos os parquet de um ano."""
    dfs = []
    for pq in list_parquets(year_dir):
        try:
            dfs.append(etl_one_parquet(pq))
        except Exception as e:
            log(f"erro lendo {pq.name}: {e}")
    df_year = safe_concat(dfs)
    log(f"ano {year_dir.name}: {df_year.shape} (linhas, colunas)")
    # Aqui você pode adicionar transformações específicas por ano, se necessário.
    return df_year

# =========================
# PIPELINE GERAl
# =========================
def run_pipeline(lang: str = "fr") -> Path:
    """
    1) varre anos em raw/
    2) concatena todos os parquet
    3) aplica dimensões no idioma escolhido
    4) salva parquet (e feather opcional)
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_years = list_year_dirs(RAW_DIR)
    if not all_years:
        raise RuntimeError(f"nenhuma pasta de ano encontrada em {RAW_DIR}")

    log(f"anos detectados: {[p.name for p in all_years]}")

    frames = []
    for y in all_years:
        frames.append(etl_one_year(y))

    fact = safe_concat(frames)
    log(f"dataset consolidado: {fact.shape}")

    # Aplica dimensões (opcional)
    fact = apply_dimensions(fact, DIMENSIONS_TO_APPLY, lang=lang)

    # Checks simples (ex.: cobertura de chaves)
    for col in DIMENSIONS_TO_APPLY.keys():
        if col in fact.columns:
            missing = fact[col].isna().sum()
            log(f"'{col}': {missing} valores NA")

    # Salva
    fact.to_parquet(FACT_OUT, index=False)
    log(f"salvo: {FACT_OUT}")

    if SAVE_FEATHER:
        fact.to_feather(FACT_OUT_FEATHER)
        log(f"salvo: {FACT_OUT_FEATHER}")

    return FACT_OUT

# =========================
# ENTRY POINT (CLI/Spyder)
# =========================
def main():
    # idioma padrão: francês; mude para "en" ou "pt"
    lang = "fr"
    # Permite: python etl_pipeline.py --lang=en
    for arg in sys.argv[1:]:
        if arg.startswith("--lang="):
            lang = arg.split("=")[-1].strip().lower()
    if lang not in {"fr","en","pt"}:
        raise ValueError("idioma inválido; use --lang=fr|en|pt")

    log(f"iniciando pipeline (idioma labels: {lang})")
    run_pipeline(lang=lang)
    log("fim")

if __name__ == "__main__":
    main()
