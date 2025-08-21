# src/build_variables_catalog.py
from pathlib import Path
import pandas as pd

# ---------- helpers de encoding ----------
def fix_mojibake(s):
    if not isinstance(s, str):
        return s
    try:
        return s.encode("latin1").decode("utf-8")
    except Exception:
        return s

# ---------- caminhos ----------
BASE   = Path(__file__).resolve().parents[1]
VARMOD = BASE / "raw" / "2022" / "varmod_MOBZELT_2022.csv"
OUTDIR = BASE / "data" / "dimensions"

# ---------- leitura ----------
def _read_varmod(path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(path, sep=";", encoding="latin-1", low_memory=False)

    expected = {"COD_VAR", "LIB_VAR", "COD_MOD", "LIB_MOD"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Colunas esperadas ausentes no varmod: {missing}")

    base = df.loc[:, ["COD_VAR", "LIB_VAR", "COD_MOD", "LIB_MOD"]].copy()
    for c in ["COD_VAR", "LIB_VAR", "COD_MOD", "LIB_MOD"]:
        base[c] = base[c].astype(str).str.strip()
        base[c] = base[c].map(fix_mojibake)

    return base.drop_duplicates().sort_values(["COD_VAR", "COD_MOD"], kind="stable")

# ---------- salvar dimensões ----------
def save_dimensions(df: pd.DataFrame):
    OUTDIR.mkdir(parents=True, exist_ok=True)
    for var, subset in df.groupby("COD_VAR"):
        # renomear colunas
        cod_col = f"COD_{var}"
        dim = subset.rename(columns={
            "COD_MOD": cod_col,
            "LIB_MOD": "Desc_lbl_fr"
        }).drop(columns=["COD_VAR", "LIB_VAR"])

        # adicionar colunas vazias de tradução
        dim["Desc_lbl_en"] = ""
        dim["Desc_lbl_pt"] = ""

        # salvar
        out_path = OUTDIR / f"Dim_{var}.csv"
        dim.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"✅ Gerado: {out_path} ({len(dim)} linhas)")

def main():
    base = _read_varmod(VARMOD)
    save_dimensions(base)

if __name__ == "__main__":
    main()
