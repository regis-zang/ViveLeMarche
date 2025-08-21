# src/build_variables_meta.py
import pandas as pd
from pathlib import Path
from encoding_helpers import fix_mojibake

BASE = Path(__file__).resolve().parents[1]
VARMOD = BASE / "raw" / "2022" / "varmod_MOBZELT_2022.csv"
OUT = BASE / "data" / "variables_meta.csv"

def read_varmod(p: Path) -> pd.DataFrame:
    # INSEE costuma ser ; e UTF-8; tentamos UTF-8 e, se falhar, latin-1
    try:
        df = pd.read_csv(p, sep=";", encoding="utf-8", low_memory=False)
    except UnicodeDecodeError:
        df = pd.read_csv(p, sep=";", encoding="latin-1", low_memory=False)

    # Esperado nesse arquivo: COD_VAR, LIB_VAR, COD_MOD, LIB_MOD
    expected = {"COD_VAR","LIB_VAR"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Colunas ausentes no varmod: {missing}")

    # Seleciona apenas o par variável/descrição e tira duplicatas
    meta = (df[["COD_VAR","LIB_VAR"]]
            .drop_duplicates()
            .rename(columns={"COD_VAR":"variable",
                             "LIB_VAR":"variable_label_fr"}))

    # Limpeza e correção de acentuação
    meta["variable"] = meta["variable"].astype(str).str.strip()
    meta["variable_label_fr"] = meta["variable_label_fr"].astype(str).str.strip()
    # Se vierem “Ã©/Ã‚” etc., corrige:
    meta["variable_label_fr"] = meta["variable_label_fr"].map(fix_mojibake)

    # Prepara colunas para traduções
    meta["variable_label_en"] = ""
    meta["variable_label_pt"] = ""

    # Ordena para facilitar revisão
    meta = meta.sort_values(["variable"], kind="stable")
    return meta

def main():
    OUT.parent.mkdir(exist_ok=True)
    meta = read_varmod(VARMOD)

    # Se já existir, preserva traduções anteriores e só atualiza FR/ordem
    if OUT.exists():
        old = pd.read_csv(OUT, dtype=str).fillna("")
        keep = old[["variable","variable_label_en","variable_label_pt"]]
        meta = (meta
                .merge(keep, on="variable", how="left", suffixes=("","_old")))
        # usa as que já existiam quando presentes
        meta["variable_label_en"] = meta["variable_label_en"].where(
            meta["variable_label_en"].astype(str).str.len()>0, meta["variable_label_en_old"]
        )
        meta["variable_label_pt"] = meta["variable_label_pt"].where(
            meta["variable_label_pt"].astype(str).str.len()>0, meta["variable_label_pt_old"]
        )
        meta = meta.drop(columns=[c for c in meta.columns if c.endswith("_old")])

    meta.to_csv(OUT, index=False, encoding="utf-8")
    # Se quiser abrir no Excel sem “???” de acentos, salve também com BOM:
    meta.to_csv(OUT.with_suffix(".utf8sig.csv"), index=False, encoding="utf-8-sig")
    print(f"Gerado: {OUT} (e {OUT.with_suffix('.utf8sig.csv')}) | {len(meta)} variáveis")

if __name__ == "__main__":
    main()
