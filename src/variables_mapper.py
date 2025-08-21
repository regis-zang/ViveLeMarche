import pandas as pd
from pathlib import Path
from typing import Dict

BASE = Path(__file__).resolve().parents[1]
CAT = BASE / "data" / "variables_catalog.csv"

def load_catalog() -> pd.DataFrame:
    df = pd.read_csv(CAT)
    # segurança
    for col in ["variable","code","label_fr","label_en","label_pt"]:
        if col not in df.columns:
            raise ValueError(f"Coluna ausente no catálogo: {col}")
    return df

def make_maps(lang: str = "fr") -> Dict[str, Dict[str, str]]:
    """Retorna dicionários {variable: {code: label_no_idioma}}"""
    assert lang in {"fr","en","pt"}, "lang deve ser fr|en|pt"
    df = load_catalog()
    label_col = {"fr":"label_fr","en":"label_en","pt":"label_pt"}[lang]

    def _label(row):
        # fallback: se EN/PT estiver vazio, cai no FR
        lab = row[label_col]
        return lab if isinstance(lab, str) and lab.strip() else row["label_fr"]

    df["_label"] = df.apply(_label, axis=1)
    maps = {}
    for var, g in df.groupby("variable", sort=False):
        maps[var] = dict(zip(g["code"].astype(str), g["_label"].astype(str)))
    return maps

def decode_series(series: pd.Series, variable: str, lang: str = "fr") -> pd.Series:
    """Converte códigos para rótulos no idioma escolhido."""
    m = make_maps(lang).get(variable, {})
    return series.astype(str).map(m).fillna(series.astype(str))
