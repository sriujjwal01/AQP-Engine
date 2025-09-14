import pandas as pd
from pathlib import Path

def load_csv(path: str, columns=None):
    
    p = Path(path)
    suf = p.suffix.lower()

    if suf == ".parquet":
        
        return pd.read_parquet(path, columns=columns)

    
    return pd.read_csv(path, usecols=columns)
