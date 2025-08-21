import pandas as pd
from pathlib import Path

# caminho raiz do projeto
base_dir = Path("I:/Projetos_Python/ViveLeMarche")
file_path = base_dir / "raw" / "2022" / "varmod_MOBZELT_2022.csv"

df_vars = pd.read_csv(file_path, sep=";", low_memory=False)
print(df_vars.head())
