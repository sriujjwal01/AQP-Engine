import argparse, time, json, statistics
from .engine import QueryEngine

def main():
    ap = argparse.ArgumentParser(description="Benchmark approx vs exact")
    ap.add_argument('--data', required=True, help='Path to CSV (used inside query)')
    ap.add_argument('--query', required=True, help='SQL-like query (must reference the same path)')
    ap.add_argument('--rates', nargs='+', type=float, default=[0.05,0.1,0.2,0.4,0.8])
    ap.add_argument('--seed', type=int, default=42)
    args = ap.parse_args()

    eng = QueryEngine()
    
    exact = eng.run(args.query, method='exact')
    exact_res = exact['result']
    exact_time = exact['time_sec']

    logs = []
    for r in args.rates:
        out = eng.run(args.query, method='stream', sample_rate=r, seed=args.seed, return_exact=False)
        approx = out['result']
        t = out['time_sec']
        
        err = rel_error(exact_res, approx)
        logs.append({'rate': r, 'time_sec': t, 'rel_error': err})

    print(json.dumps({
        'exact_time_sec': exact_time,
        'exact_rows': exact_res,
        'runs': logs
    }, indent=2))

def rel_error(exact, approx):
    
    def to_map(rows):
        if len(rows)==1 and len(rows[0])==1:
            
            k = ('__single__',)
            v = list(rows[0].values())[0]
            return {k:v}
        d = {}
        for r in rows:
            keys = tuple((k,v) for k,v in r.items() if not any(a in k for a in ['COUNT','SUM','AVG']))
            val_key = [k for k in r.keys() if any(a in k for a in ['COUNT','SUM','AVG'])][0]
            d[keys] = r[val_key]
        return d
    me = to_map(exact)
    ma = to_map(approx)
    errs = []
    for k, v in me.items():
        if k in ma:
            denom = abs(v) if v!=0 else 1.0
            errs.append(abs(ma[k]-v)/denom)
    if not errs:
        return None
    return sum(errs)/len(errs)

if __name__ == '__main__':
    main()
