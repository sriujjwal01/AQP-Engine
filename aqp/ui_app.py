import sys, pathlib
root = pathlib.Path(__file__).resolve().parents[2] 
if str(root) not in sys.path:
    sys.path.insert(0, str(root))
import streamlit as st
import time
import json, csv
import pandas as pd
import matplotlib.pyplot as plt


try:
    from aqp_engine.aqp.engine import QueryEngine
except ModuleNotFoundError:
    import sys, pathlib
    root = pathlib.Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from aqp_engine.aqp.engine import QueryEngine

st.set_page_config(page_title="TrendForge AQP Engine", layout="wide")

st.title("⚡ TrendForge — Approximate Query Engine (AQP)")
st.write("Speed vs accuracy for analytics — compare approximate vs exact.")


st.subheader("Data Source")
source_mode = st.radio("Choose data source", ["Upload file", "Local path"], horizontal=True)

selected_path = None

if source_mode == "Upload file":
    uploaded = st.file_uploader("Upload CSV / CSV.GZ", type=["csv", "gz"])
    if uploaded:
       
        fname = uploaded.name
        with open(fname, "wb") as f:
            f.write(uploaded.getvalue())
        selected_path = fname
        st.success(f"Saved upload to: {fname}")
else:
    selected_path = st.text_input(
        "Local path to data file (CSV / CSV.GZ / Parquet)",
        value=r"C:\data\large_10M.csv"  # change to your path
    )
    st.caption("Tip: Use .csv.gz (compressed) or .parquet for faster I/O and smaller files.")


default_query = "SELECT city, SUM(amount) FROM uploaded.csv GROUP BY city"
sql = st.text_area("SQL-like query:", value=default_query, height=100)

method = st.selectbox("Approximation method", ["sample", "stream", "exact"], index=1)
rate = st.slider("Sample rate (for 'sample' or 'stream')", 0.01, 1.0, 0.1, 0.01)
seed = st.number_input("Seed", value=42, step=1)
show_exact = st.checkbox("Also compute exact for comparison", value=False)  # default off for timing fairness


def normalize_sql_from(sql_text: str, path: str | None) -> str:
    if not path:
        return sql_text
    token_upper = "FROM uploaded.csv"
    token_lower = "from uploaded.csv"
    if token_upper in sql_text or token_lower in sql_text:
        return sql_text.replace("uploaded.csv", path)
    return sql_text


if st.button("Run"):
    if not selected_path:
        st.error("Please upload a file or provide a valid local path.")
    else:
        
        sql_norm = normalize_sql_from(sql, selected_path)

        eng = QueryEngine()
        t0 = time.time()
        out = eng.run(sql_norm, method=method, sample_rate=rate, seed=int(seed), return_exact=show_exact)
        t1 = time.time()

        st.subheader("Approximate Result")
        st.code(out, language="json")
        st.caption(f"Ran in {out['time_sec']:.3f}s (engine), {t1-t0:.3f}s (UI total)")

        if show_exact and "exact" in out:
            st.subheader("Exact Result")
            st.code(out["exact"], language="json")


def _rel_error(exact, approx):
    def to_map(rows):
        if len(rows) == 1 and len(rows[0]) == 1:
            return {('__single__',): list(rows[0].values())[0]}
        d = {}
        for r in rows:
            keys = tuple((k, v) for k, v in r.items() if not any(a in k for a in ["COUNT", "SUM", "AVG"]))
            val_key = [k for k in r.keys() if any(a in k for a in ["COUNT", "SUM", "AVG"])][0]
            d[keys] = r[val_key]
        return d
    me = to_map(exact); ma = to_map(approx)
    errs = []
    for k, v in me.items():
        if k in ma:
            denom = abs(v) if v != 0 else 1.0
            errs.append(abs(ma[k] - v) / denom)
    return None if not errs else sum(errs) / len(errs)

st.markdown("---")
st.header("Benchmark")

st.write("Run one query at multiple sample rates to see time vs error. "
         "Use a large local file for clear speedups. Uncheck 'Also compute exact' above when timing approx only.")

rates_str = st.text_input("Sample rates (comma-separated)", value="0.05,0.1,0.2,0.4")
seed_bm = st.number_input("Seed (benchmark)", value=42, step=1)
run_bench = st.button("Run Benchmark")

if run_bench:
    if not selected_path:
        st.error("Please select a data source first.")
    else:
        sql_norm = normalize_sql_from(sql, selected_path)
        eng = QueryEngine()

        
        exact = eng.run(sql_norm, method="exact")
        exact_res = exact["result"]
        exact_time = exact["time_sec"]

        
        try:
            rates = [float(x.strip()) for x in rates_str.split(",") if x.strip()]
            rates = [r for r in rates if 0 < r <= 1.0]
        except Exception:
            st.error("Could not parse sample rates. Use values like: 0.05,0.1,0.2")
            rates = []

        logs = []
        for r in rates:
            out = eng.run(sql_norm, method="stream", sample_rate=r, seed=int(seed_bm))
            logs.append({"rate": r, "time_sec": out["time_sec"], "rel_error": _rel_error(exact_res, out["result"])})

        st.subheader("Benchmark Results (table)")
        st.dataframe(pd.DataFrame(logs))

       
        fig1 = plt.figure()
        plt.plot([x["rate"] for x in logs], [x["time_sec"] for x in logs], marker="o")
        plt.xlabel("Sample rate (p)")
        plt.ylabel("Time (sec)")
        plt.title("Runtime vs Sample Rate")
        st.pyplot(fig1)

        fig2 = plt.figure()
        plt.plot([x["rate"] for x in logs], [x["rel_error"] for x in logs], marker="o")
        plt.xlabel("Sample rate (p)")
        plt.ylabel("Average relative error")
        plt.title("Relative Error vs Sample Rate")
        st.pyplot(fig2)

        
        bench_payload = {"exact_time_sec": exact_time, "runs": logs, "query": sql_norm}
        bench_json = json.dumps(bench_payload, indent=2).encode()
        st.download_button("Download benchmark JSON", bench_json, file_name="benchmark_results.json", mime="application/json")

        import io as _io
        csv_buf = _io.StringIO()
        w = csv.writer(csv_buf); w.writerow(["rate","time_sec","rel_error"])
        for r in logs: w.writerow([r["rate"], r["time_sec"], r["rel_error"]])
        st.download_button("Download benchmark CSV", csv_buf.getvalue().encode(), file_name="benchmark_results.csv", mime="text/csv")
