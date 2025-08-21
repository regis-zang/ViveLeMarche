import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
VARMOD = BASE / "raw" / "2022" / "varmod_MOBZELT_2022.csv"
OUT = BASE / "data" / "variables_catalog.csv"

def _read_varmod(path: Path) -> pd.DataFrame:
    # INSEE usa ; e geralmente UTF-8
    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=";", encoding="latin-1", low_memory=False)

    expected = {"COD_VAR","LIB_VAR","COD_MOD","LIB_MOD"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Colunas esperadas ausentes no varmod: {missing}")

    base = df.loc[:, ["COD_VAR","LIB_VAR","COD_MOD","LIB_MOD"]].rename(columns={
        "COD_VAR": "variable",
        "LIB_VAR": "variable_label_fr",
        "COD_MOD": "code",
        "LIB_MOD": "label_fr",
    })

    # limpeza
    for c in ["variable","variable_label_fr","code","label_fr"]:
        base[c] = base[c].astype(str).str.strip()

    base = base.drop_duplicates().sort_values(["variable","code"], kind="stable")
    return base

def main():
    OUT.parent.mkdir(exist_ok=True)
    base = _read_varmod(VARMOD)

    if OUT.exists():
        cat = pd.read_csv(OUT)
        # preserva traduções já existentes
        keep_cols = [c for c in ["label_en","label_pt"] if c in cat.columns]
        merged = base.merge(cat[["variable","code"] + keep_cols], how="left", on=["variable","code"])
        for col in ["label_en","label_pt"]:
            if col not in merged.columns:
                merged[col] = ""
    else:
        merged = base.copy()
        merged["label_en"] = ""
        merged["label_pt"] = ""

    # ordena para facilitar revisão
    merged = merged.sort_values(["variable","code"], kind="stable")
    merged.to_csv(OUT, index=False, encoding="utf-8")
    print(f"Catálogo salvo: {OUT} | Linhas: {len(merged)}")

if __name__ == "__main__":
    main()
