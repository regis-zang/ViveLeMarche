from pathlib import Path
import time
import pandas as pd
import re

# -------- tradução automática (Google) --------
try:
    from deep_translator import GoogleTranslator
    HAS_TRANSLATOR = True
except Exception:
    HAS_TRANSLATOR = False

BASE = Path(__file__).resolve().parents[1]
DIM_DIR = BASE / "data" / "dimensions"
CACHE   = BASE / "data" / "_dim_translation_cache.csv"

def fix_mojibake(s):
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except Exception:
        return s

def translate(text: str, target: str, source: str = "fr", sleep: float = 0.10) -> str:
    """
    Traduz texto FR -> target ('en' ou 'pt'). Se falhar, retorna original.
    Pequeno sleep para evitar limite de taxa.
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

# Normalizações para EN-UK (simples e seguras)
UK_FIXES = [
    (r"\bLabor\b", "Labour"),
    (r"\blabor\b", "labour"),
    (r"\bCenter\b", "Centre"),
    (r"\bcenter\b", "centre"),
    (r"\bCenters\b", "Centres"),
    (r"\bcenters\b", "centres"),
    (r"\bProgram\b", "Programme"),
    (r"\bprogram\b", "programme"),
    (r"\bTransportation\b", "Transport"),
    (r"\btransportation\b", "transport"),
    # preferimos “age” vs “years old”: “15 ans” => “15 years”
    (r"\byears old\b", "years"),
]

def to_en_uk(s: str) -> str:
    if not isinstance(s, str):
        return s
    out = s
    for pat, rep in UK_FIXES:
        out = re.sub(pat, rep, out)
    return out

def load_cache() -> pd.DataFrame:
    if CACHE.exists():
        return pd.read_csv(CACHE, dtype=str).fillna("")
    return pd.DataFrame(columns=["Desc_lbl_fr","Desc_lbl_en","Desc_lbl_pt"])

def save_cache(df_cache: pd.DataFrame):
    df_cache.drop_duplicates(subset=["Desc_lbl_fr"], inplace=True)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    df_cache.to_csv(CACHE, index=False, encoding="utf-8")

def translate_file(path: Path, cache_df: pd.DataFrame) -> int:
    """
    Traduz um Dim_*.csv in-place. Retorna quantidade de linhas traduzidas.
    """
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str).fillna("")
    fr_col = "Desc_lbl_fr"

    # sanity check: presença das colunas
    for col in [fr_col, "Desc_lbl_en", "Desc_lbl_pt"]:
        if col not in df.columns:
            raise ValueError(f"Coluna ausente em {path.name}: {col}")

    # corrige acentos (se necessário)
    df[fr_col] = df[fr_col].map(fix_mojibake)

    # mapas de cache
    cache_en = dict(zip(cache_df["Desc_lbl_fr"], cache_df.get("Desc_lbl_en", "")))
    cache_pt = dict(zip(cache_df["Desc_lbl_fr"], cache_df.get("Desc_lbl_pt", "")))

    need_en = df["Desc_lbl_en"].str.strip().eq("")
    need_pt = df["Desc_lbl_pt"].str.strip().eq("")
    todo = df[need_en | need_pt]
    translated_rows = 0

    for idx, row in todo.iterrows():
        src = row[fr_col].strip()
        if not src:
            continue

        # EN-UK
        if need_en.loc[idx]:
            en = cache_en.get(src, "")
            if not en:
                en = translate(src, "en")
                en = to_en_uk(en)
                cache_en[src] = en
                translated_rows += 1
            df.at[idx, "Desc_lbl_en"] = en

        # PT-BR
        if need_pt.loc[idx]:
            pt = cache_pt.get(src, "")
            if not pt:
                pt = translate(src, "pt")
                cache_pt[src] = pt
                translated_rows += 1
            df.at[idx, "Desc_lbl_pt"] = pt

    # salva de volta (UTF-8 com BOM para Excel)
    df.to_csv(path, index=False, encoding="utf-8-sig")

    # atualiza cache em memória
    merged_keys = set(cache_en.keys()) | set(cache_pt.keys())
    cache_df = pd.DataFrame({"Desc_lbl_fr": list(merged_keys)})
    cache_df["Desc_lbl_en"] = cache_df["Desc_lbl_fr"].map(cache_en).fillna("")
    cache_df["Desc_lbl_pt"] = cache_df["Desc_lbl_fr"].map(cache_pt).fillna("")
    save_cache(cache_df)

    return translated_rows

def main():
    if not HAS_TRANSLATOR:
        print("deep-translator not installed. Install with: pip install deep-translator")
        return

    if not DIM_DIR.exists():
        print(f"Directory not found: {DIM_DIR}")
        return

    cache_df = load_cache()
    total_files = 0
    total_trans = 0

    for csv_path in sorted(DIM_DIR.glob("Dim_*.csv")):
        total_files += 1
        n = translate_file(csv_path, cache_df)
        total_trans += n
        print(f"Translated {n:4d} rows in {csv_path.name}")

        # reload cache after each file (keeps memory small and always up-to-date)
        cache_df = load_cache()

    print(f"Done. Files: {total_files}, Translated rows: {total_trans}")

if __name__ == "__main__":
    main()
