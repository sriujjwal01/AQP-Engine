from __future__ import annotations 
import time
from typing import Optional, Dict, Any
import pandas as pd
import numpy as np

from .parser import parse
from .sampling import uniform_sample_df
from .data import load_csv


class QueryEngine:
    
    def __init__(self) -> None:
        pass

    def run(
        self,
        sql: str,
        method: str = "sample",             
        sample_rate: float = 0.1,           
        seed: Optional[int] = None,
        streaming_chunksize: int = 1_000_000,
        return_exact: bool = False
    ) -> Dict[str, Any]:
        q = parse(sql)
        t0 = time.time()

        if method == "exact":
            exact = self._run_exact(q)
            return {"mode": "exact", "time_sec": time.time() - t0, "result": exact}

        if method == "sample":
            
            df_full = load_csv(q.source)
            df_full = self._apply_where(df_full, q)
            df_samp = uniform_sample_df(df_full, sample_rate, seed)
            res = self._aggregate(df_samp, q, scale=(1.0 / max(sample_rate, 1e-12)))
            out = {"mode": "sample", "time_sec": time.time() - t0, "result": res}
            if return_exact:
                et0 = time.time()
                exact = self._run_exact(q)
                out["exact"] = {"time_sec": time.time() - et0, "result": exact}
            return out

        if method == "stream":
            res = self._stream_approx(q, p=sample_rate, seed=seed, chunksize=streaming_chunksize)
            out = {"mode": "stream", "time_sec": time.time() - t0, "result": res}
            if return_exact:
                et0 = time.time()
                exact = self._run_exact(q)
                out["exact"] = {"time_sec": time.time() - et0, "result": exact}
            return out

        raise ValueError("Unknown method: " + method)

    

    def _apply_where(self, df: pd.DataFrame, q):
        if q.where_col:
            val = q.where_val
            if isinstance(val, str) and len(val) >= 2 and ((val[0] == "'" and val[-1] == "'") or (val[0] == '"' and val[-1] == '"')):
                val = val[1:-1]
            op = q.where_op
            v = _coerce(df, q.where_col, val)
            if op == '=':  return df[df[q.where_col] == v]
            if op == '!=': return df[df[q.where_col] != v]
            if op == '>':  return df[df[q.where_col] >  v]
            if op == '<':  return df[df[q.where_col] <  v]
            if op == '>=': return df[df[q.where_col] >= v]
            if op == '<=': return df[df[q.where_col] <= v]
        return df

    def _aggregate(self, df: pd.DataFrame, q, scale: float = 1.0):
        agg = q.agg
        col = q.agg_col
        by = q.group_by or q.select_cols

        if not by:
            if agg.startswith('COUNT'):
                val = (df[col].count() if col and col != '*' else len(df))
                return [{agg: float(val * scale)}]
            elif agg.startswith('SUM'):
                val = df[col].sum()
                return [{f"SUM({col})": float(val * scale)}]
            elif agg.startswith('AVG'):
                val = df[col].mean()
                return [{f"AVG({col})": float(val)}]

       
        g = df.groupby(by, dropna=False)
        rows = []
        if agg.startswith('COUNT'):
            if col and col != '*':
                s = g[col].count() * scale
                for k, v in s.items():
                    rows.append(_row(by, k) | {agg: float(v)})
            else:
                s = g.size() * scale
                for k, v in s.items():
                    rows.append(_row(by, k) | {agg: float(v)})
        elif agg.startswith('SUM'):
            s = g[col].sum() * scale
            for k, v in s.items():
                rows.append(_row(by, k) | {f"SUM({col})": float(v)})
        elif agg.startswith('AVG'):
            s = g[col].mean()
            for k, v in s.items():
                rows.append(_row(by, k) | {f"AVG({col})": float(v)})
        return rows

    def _run_exact(self, q):
        df = load_csv(q.source)
        df = self._apply_where(df, q)
        return self._aggregate(df, q, scale=1.0)

   
    def _needed_columns(self, q) -> list[str] | None:
        cols = set()
       
        for c in (q.group_by or q.select_cols or []):
            if c and c != '*':
                cols.add(c)
       
        if q.agg_col and q.agg_col != '*':
            cols.add(q.agg_col)
       
        if q.where_col:
            cols.add(q.where_col)
        return list(cols) if cols else None  # None => read all

    def _stream_approx(self, q, p: float, seed: Optional[int], chunksize: int):
        
        rng = np.random.default_rng(seed)
        usecols = self._needed_columns(q)

        
        dtypes = None
        if usecols:
            dtypes = {}
            for c in usecols:
                if c == (q.agg_col or "") and (q.agg.startswith("SUM") or q.agg.startswith("AVG")):
                    dtypes[c] = "float64"
                elif "id" in c.lower() or "clicked" in c.lower():
                    dtypes[c] = "int64"
                else:
                    dtypes[c] = "object"

        by = q.group_by or q.select_cols
        agg = q.agg
        col = q.agg_col

       
        grouped_counts: dict = {}
        grouped_sums: dict = {}

        for chunk in pd.read_csv(
            q.source,
            usecols=usecols,
            chunksize=chunksize,
            dtype=dtypes,
            low_memory=False,
            engine="c",
            memory_map=True,
        ):
            
            if q.where_col:
                val = q.where_val
                if isinstance(val, str) and len(val) >= 2 and ((val[0] == "'" and val[-1] == "'") or (val[0] == '"' and val[-1] == '"')):
                    val = val[1:-1]
                series = chunk[q.where_col]
                try:
                    if "int" in str(series.dtype):   val = int(val)
                    elif "float" in str(series.dtype): val = float(val)
                except:  
                    pass
                op = q.where_op
                if   op == "=":  chunk = chunk[series == val]
                elif op == "!=": chunk = chunk[series != val]
                elif op == ">":  chunk = chunk[series >  val]
                elif op == "<":  chunk = chunk[series <  val]
                elif op == ">=": chunk = chunk[series >= val]
                elif op == "<=": chunk = chunk[series <= val]
            if chunk.empty:
                continue

         
            mask = rng.random(len(chunk)) < p
            if not mask.any():
                continue
            samp = chunk.loc[mask]

            if not by:
                if agg.startswith("COUNT"):
                    grouped_counts[None] = grouped_counts.get(None, 0) + len(samp)
                elif agg.startswith("SUM"):
                    grouped_sums[None] = grouped_sums.get(None, 0.0) + float(samp[col].sum())
                elif agg.startswith("AVG"):
                    grouped_sums[None] = grouped_sums.get(None, 0.0) + float(samp[col].sum())
                    grouped_counts[None] = grouped_counts.get(None, 0) + int(samp[col].count())
            else:
                g = samp.groupby(by, dropna=False)
                if agg.startswith("COUNT"):
                    s = g.size()
                    for k, v in s.items():
                        key = _key_tuple(k)
                        grouped_counts[key] = grouped_counts.get(key, 0) + int(v)
                elif agg.startswith("SUM"):
                    s = g[col].sum()
                    for k, v in s.items():
                        key = _key_tuple(k)
                        grouped_sums[key] = grouped_sums.get(key, 0.0) + float(v)
                elif agg.startswith("AVG"):
                    c = g[col].count()
                    s = g[col].sum()
                    for k in s.index:
                        key = _key_tuple(k)
                        grouped_sums[key] = grouped_sums.get(key, 0.0) + float(s.loc[k])
                        grouped_counts[key] = grouped_counts.get(key, 0) + int(c.loc[k])

       
        rows = []
        scale = 1.0 / max(p, 1e-12)

        if not by:
            if agg.startswith("COUNT"):
                rows.append({agg: float(grouped_counts.get(None, 0) * scale)})
            elif agg.startswith("SUM"):
                rows.append({f"SUM({col})": float(grouped_sums.get(None, 0.0) * scale)})
            elif agg.startswith("AVG"):
                s = grouped_sums.get(None, 0.0)
                c = grouped_counts.get(None, 0)
                rows.append({f"AVG({col})": float(s / c) if c else float("nan")})
            return rows

        if agg.startswith("COUNT"):
            for key, cnt in grouped_counts.items():
                rows.append(_row(by, key) | {agg: float(cnt * scale)})
        elif agg.startswith("SUM"):
            for key, sm in grouped_sums.items():
                rows.append(_row(by, key) | {f"SUM({col})": float(sm * scale)})
        elif agg.startswith("AVG"):
            keys = set(grouped_sums) | set(grouped_counts)
            for key in keys:
                s = grouped_sums.get(key, 0.0)
                c = grouped_counts.get(key, 0)
                rows.append(_row(by, key) | {f"AVG({col})": float(s / c) if c else float("nan")})
        return rows



def _row(cols, key):
    if not isinstance(key, tuple):
        key = (key,)
    return {c: k for c, k in zip(cols, key)}

def _coerce(df: pd.DataFrame, col: str, val: str):
    dt = df[col].dtype
    try:
        if 'int' in str(dt):   return int(val)
        if 'float' in str(dt): return float(val)
    except:
        pass
    if isinstance(val, str) and len(val) >= 2 and ((val[0] == "'" and val[-1] == "'") or (val[0] == '"' and val[-1] == '"')):
        return val[1:-1]
    return val

def _key_tuple(k) -> tuple:
    return k if isinstance(k, tuple) else (k,)
