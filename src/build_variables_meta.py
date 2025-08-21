from pathlib import Path
import time
import pandas as pd

# --- helpers de encoding (corrigir mojibake) -------------------------------
def fix_mojibake(s):
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except Exception:
        return s

# --- tradução automática (Google) -----------------------------------------
try:
    from deep_translator import GoogleTranslator
    HAS_TRANSLATOR = True
except Exception:
    HAS_TRANSLATOR = False

def translate(text: str, target: str, source: str = "fr", sleep: float = 0.12) -> str:
    """
    Traduz FR -> target ('en' ou 'pt'). Se algo falhar, devolve o original.
    Pequeno sleep para evitar rate limit.
    """
    if not HAS_TRANSLATOR or not isinstance(text, str) or not text.strip():
        return text
    try:
        out = GoogleTranslator(source=source, target=target).translate(text)
        if sleep:
            time.sleep(sleep)
        return out
    except Exception:
        return text

# --- caminhos --------------------------------------------------------------
BASE = Path(__file__).resolve().parents[1]
INFILE = BASE / "raw" / "2022" / "varmod_MOBZELT_2022.csv"
OUT    = BASE / "data" / "variables_meta.csv"

def safe_read_varmod(path: Path) -> pd.DataFrame:
    # INSEE costuma usar ; e UTF‑8; tenta UTF‑8 e cai para Latin‑1 se preciso
    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=";", encoding="latin-1", low_memory=False)

    expected = {"COD_VAR", "LIB_VAR"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes no varmod: {missing}")

    meta = (df[["COD_VAR", "LIB_VAR"]]
            .drop_duplicates()
            .rename(columns={"COD_VAR": "variable",
                             "LIB_VAR": "variable_label_fr"}))

    # limpeza e acentuação
    meta["variable"] = meta["variable"].astype(str).str.strip()
    meta["variable_label_fr"] = (meta["variable_label_fr"]
                                 .astype(str).str.strip()
                                 .map(fix_mojibake))

    # cria colunas de tradução
    meta["variable_label_en"] = ""
    meta["variable_label_pt"] = ""

    return meta.sort_values("variable", kind="stable")

def fill_translations(df: pd.DataFrame) -> pd.DataFrame:
    # somente onde estiver vazio (preserva edições manuais/re-execuções)
    m_en = df["variable_label_en"].astype(str).str.strip().eq("")
    df.loc[m_en, "variable_label_en"] = df.loc[m_en, "variable_label_fr"]\
        .apply(lambda x: translate(x, target="en"))

    m_pt = df["variable_label_pt"].astype(str).str.strip().eq("")
    df.loc[m_pt, "variable_label_pt"] = df.loc[m_pt, "variable_label_fr"]\
        .apply(lambda x: translate(x, target="pt"))

    return df

def main():
    OUT.parent.mkdir(exist_ok=True)
    meta = safe_read_varmod(INFILE)

    # se já existe um catálogo, preserva traduções já feitas
    if OUT.exists():
        old = pd.read_csv(OUT, dtype=str).fillna("")
        keep = old[["variable", "variable_label_en", "variable_label_pt"]]
        meta = (meta.merge(keep, on="variable", how="left", suffixes=("", "_old")))
        for col in ("variable_label_en", "variable_label_pt"):
            meta[col] = meta[col].where(meta[col].str.len() > 0, meta[f"{col}_old"])
        meta.drop(columns=[c for c in meta.columns if c.endswith("_old")], inplace=True)

    # traduz (se deep-translator estiver disponível)
    if HAS_TRANSLATOR:
        meta = fill_translations(meta)
    else:
        print("⚠️ deep-translator não instalado — pulando tradução automática.")

    # ordena e salva (UTF‑8 + UTF‑8 BOM p/ Excel)
    meta = meta.sort_values("variable", kind="stable")
    meta.to_csv(OUT, index=False, encoding="utf-8")
    meta.to_csv(OUT.with_suffix(".utf8sig.csv"), index=False, encoding="utf-8-sig")
    print(f"✅ Gerado: {OUT.name} e {OUT.with_suffix('.utf8sig.csv').name} | {len(meta)} variáveis")

if __name__ == "__main__":
    main()
