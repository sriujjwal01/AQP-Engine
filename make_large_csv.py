import numpy as np
import pandas as pd

rows = 100_000_000  
chunksize = 5_000_000
rng = np.random.default_rng(123)

cities = ["Delhi","Mumbai","Bengaluru","Hyderabad","Chennai","Pune","Kolkata"]
probs = [0.16,0.18,0.2,0.14,0.12,0.1,0.1]

path = "large_50M.csv"

with open(path, "w", encoding="utf-8") as f:
    f.write("user_id,city,amount,clicked\n")

for start in range(0, rows, chunksize):
    size = min(chunksize, rows - start)
    city = rng.choice(cities, size=size, p=probs)
    user_id = rng.integers(1, 10_000_000, size=size)
    amount = np.round(rng.gamma(shape=2.0, scale=150.0, size=size), 2)
    clicked = rng.choice([0,1], size=size, p=[0.78,0.22])
    df = pd.DataFrame({
        "user_id": user_id,
        "city": city,
        "amount": amount,
        "clicked": clicked
    })
    df.to_csv(path, mode="a", header=False, index=False)
    print(f"Wrote {start+size:,} rows...")
