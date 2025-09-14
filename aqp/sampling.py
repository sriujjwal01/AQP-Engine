import random
import math
import pandas as pd

def uniform_sample_df(df: pd.DataFrame, frac: float, seed: int | None = None) -> pd.DataFrame:
    if frac >= 1.0:
        return df
    return df.sample(frac=frac, random_state=seed)

class Reservoir:
    
    def __init__(self, k: int, seed: int | None = None):
        self.k = k
        self.n = 0
        self.res = []
        self.rand = random.Random(seed)

    def feed(self, row):
        self.n += 1
        if len(self.res) < self.k:
            self.res.append(row)
        else:
            j = self.rand.randint(1, self.n)
            if j <= self.k:
                idx = self.rand.randint(0, self.k-1)
                self.res[idx] = row

    def to_dataframe(self, columns):
        return pd.DataFrame(self.res, columns=columns)

def reservoir_from_csv(path: str, k: int, seed: int | None = None) -> pd.DataFrame:
    import csv
    r = Reservoir(k, seed=seed)
    with open(path, newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            r.feed(row)
    return r.to_dataframe(header)
